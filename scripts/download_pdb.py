import os
import requests
import concurrent.futures
import time
from typing import List, Dict, Tuple
from datetime import datetime

class PDBDownloader:
    def __init__(self):
        # Create output directories
        self.output_dir = "datasets/raw/pdb"
        self.ids_dir = "datasets/raw/pdb_ids"
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.ids_dir, exist_ok=True)

        # Download failure configurations
        self.failed_downloads: Dict[str, DownloadFailure] = {}
        self.MAX_RETRY_ATTEMPTS = 3
        self.RETRY_WAIT_TIME = 300  # 5 minutes before retry
        self.max_workers = 20  # Maximum number of parallel download threads

    def fetch_pdb_ids(self, cutoff_date: str = "2020-01-01", batch_size: int = 1000) -> List[str]:
        """Fetch all PDB IDs before the specified cutoff date"""
        print(f"Fetching PDB IDs for entries before {cutoff_date}...")
        
        url = "https://search.rcsb.org/rcsbsearch/v2/query"
        query_template = {
            "query": {
                "type": "terminal",
                "service": "text",
                "parameters": {
                    "attribute": "rcsb_accession_info.initial_release_date",
                    "operator": "less",
                    "value": cutoff_date
                }
            },
            "return_type": "entry",
            "request_options": {
                "paginate": {
                    "start": 0,
                    "rows": 100
                },
                "sort": [
                    {
                        "sort_by": "rcsb_accession_info.initial_release_date",
                        "direction": "desc"
                    }
                ]
            }
        }

        all_pdb_ids = []
        start = 0
        rows = 100
        batch_count = 0

        while True:
            query_template["request_options"]["paginate"]["start"] = start
            query_template["request_options"]["paginate"]["rows"] = rows

            try:
                response = requests.post(url, json=query_template)
                response.raise_for_status()
                
                result_set = response.json().get("result_set", [])
                if not result_set:
                    break

                pdb_ids = [entry["identifier"] for entry in result_set]
                all_pdb_ids.extend(pdb_ids)
                print(f"Fetched {len(pdb_ids)} PDB entries (total: {len(all_pdb_ids)}).")

                start += rows

                # Save IDs to file when reaching batch size
                if len(all_pdb_ids) >= (batch_count + 1) * batch_size:
                    self._save_batch_ids(all_pdb_ids, batch_count, batch_size)
                    batch_count += 1

            except requests.exceptions.RequestException as e:
                print(f"Error fetching PDB IDs: {e}")
                break

        # Save remaining IDs
        if len(all_pdb_ids) > batch_count * batch_size:
            self._save_batch_ids(all_pdb_ids, batch_count, batch_size)

        print(f"Found {len(all_pdb_ids)} PDB entries in total.")
        return all_pdb_ids

    def _save_batch_ids(self, all_ids: List[str], batch_count: int, batch_size: int) -> None:
        """Save a batch of PDB IDs to file"""
        batch_ids = all_ids[batch_count * batch_size:(batch_count + 1) * batch_size]
        batch_file = os.path.join(self.ids_dir, f"pdb_ids_batch_{batch_count}.txt")
        with open(batch_file, "w") as f:
            f.write("\n".join(batch_ids))
        print(f"Saved batch {batch_count} with {len(batch_ids)} IDs to {batch_file}")

    def download_structures(self) -> None:
        """Download all PDB structure files"""
        print("\nStarting structure downloads...")
        batch_files = sorted([f for f in os.listdir(self.ids_dir) if f.startswith("pdb_ids_batch_")])
        total_batches = len(batch_files)

        for batch_index, batch_file in enumerate(batch_files, 1):
            print(f"\nProcessing batch {batch_index}/{total_batches} "
                  f"({batch_index/total_batches*100:.1f}%) - {batch_file}")
            
            with open(os.path.join(self.ids_dir, batch_file), "r") as f:
                pdb_ids = [line.strip() for line in f.readlines()]
            
            print(f"Current batch contains {len(pdb_ids)} IDs")
            self._process_batch(pdb_ids)
            
            print(f"Completed batch file {batch_file}")
            remaining_batches = total_batches - batch_index
            print(f"Remaining batches: {remaining_batches}")
            print(f"Current failed downloads: {len(self.failed_downloads)}")
            
            if remaining_batches > 0:
                print("Taking a short break before next batch...")
                time.sleep(2)

        # Process failed downloads
        if self.failed_downloads:
            print(f"\nProcessing {len(self.failed_downloads)} failed downloads...")
            self._retry_failed_downloads()

        # Output final failure statistics
        self._save_failure_summary()

    def _download_single_file(self, pdb_id: str) -> Tuple[bool, str]:
        """Download a single PDB file"""
        cif_url = f"https://files.rcsb.org/download/{pdb_id}.cif"
        try:
            response = requests.get(cif_url, timeout=30)
            if response.status_code == 200:
                with open(os.path.join(self.output_dir, f"{pdb_id}.cif"), "wb") as f:
                    f.write(response.content)
                return True, f"Successfully downloaded {pdb_id}.cif"
            else:
                error_msg = f"Failed to download {pdb_id}.cif, status code: {response.status_code}"
                return False, error_msg
        except Exception as e:
            error_msg = f"Error downloading {pdb_id}.cif: {str(e)}"
            return False, error_msg

    def _process_batch(self, pdb_ids: List[str]) -> None:
        """Process a batch of PDB IDs for downloading"""
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_pdb = {executor.submit(self._download_single_file, pdb_id): pdb_id 
                           for pdb_id in pdb_ids}
            
            completed = 0
            total = len(future_to_pdb)
            
            for future in concurrent.futures.as_completed(future_to_pdb):
                pdb_id = future_to_pdb[future]
                try:
                    success, result = future.result()
                    completed += 1
                    if not success:
                        self._record_failure(pdb_id, result)
                    
                    if completed % 10 == 0 or completed == total:
                        print(f"Batch progress: {completed}/{total} "
                              f"({completed/total*100:.1f}%) - {result}")
                except Exception as e:
                    print(f"Error processing {pdb_id}: {str(e)}")

    def _record_failure(self, pdb_id: str, error_msg: str) -> None:
        """Record a failed download attempt"""
        if pdb_id in self.failed_downloads:
            self.failed_downloads[pdb_id].attempt += 1
            self.failed_downloads[pdb_id].error_msg = error_msg
            self.failed_downloads[pdb_id].timestamp = time.time()
        else:
            self.failed_downloads[pdb_id] = DownloadFailure(pdb_id, error_msg)

    def _retry_failed_downloads(self) -> None:
        """Retry failed downloads after waiting period"""
        while self.failed_downloads:
            current_time = time.time()
            retry_pdb_ids = [
                pdb_id for pdb_id, failure in self.failed_downloads.items()
                if failure.attempt < self.MAX_RETRY_ATTEMPTS and 
                (current_time - failure.timestamp) >= self.RETRY_WAIT_TIME
            ]
            
            if not retry_pdb_ids:
                print("Waiting for retry cooldown...")
                time.sleep(60)
                continue
                
            print(f"\nRetrying {len(retry_pdb_ids)} failed downloads...")
            self._process_batch(retry_pdb_ids)
            
            # Clean up completed or max-attempted entries
            for pdb_id in list(self.failed_downloads.keys()):
                if pdb_id not in retry_pdb_ids:
                    continue
                if self.failed_downloads[pdb_id].attempt >= self.MAX_RETRY_ATTEMPTS:
                    print(f"Maximum retry attempts reached for {pdb_id}")
                    del self.failed_downloads[pdb_id]
                elif os.path.exists(os.path.join(self.output_dir, f"{pdb_id}.cif")):
                    del self.failed_downloads[pdb_id]

    def _save_failure_summary(self) -> None:
        """Save summary of failed downloads"""
        if not self.failed_downloads:
            return

        print("\nFinal failed downloads summary:")
        for pdb_id, failure in self.failed_downloads.items():
            print(f"- {pdb_id}: Failed {failure.attempt} times. Last error: {failure.error_msg}")
        
        failure_file = os.path.join("datasets", "failed_downloads_pdb_cif.txt")
        with open(failure_file, "w") as f:
            for pdb_id, failure in self.failed_downloads.items():
                f.write(f"{pdb_id}\t{failure.attempt}\t{failure.error_msg}\n")
        print(f"\nFailed downloads have been saved to '{failure_file}'")

class DownloadFailure:
    """Class for tracking download failures"""
    def __init__(self, pdb_id: str, error_msg: str, attempt: int = 1):
        self.pdb_id = pdb_id
        self.error_msg = error_msg
        self.attempt = attempt
        self.timestamp = time.time()

def main():
    start_time = time.time()
    print(f"Starting PDB data download process at {datetime.now()}")
    
    downloader = PDBDownloader()
    
    # Step 1: Fetch PDB IDs
    downloader.fetch_pdb_ids()
    
    # Step 2: Download structure files
    downloader.download_structures()
    
    end_time = time.time()
    duration = end_time - start_time
    print(f"\nDownload process completed in {duration/3600:.2f} hours")
    print(f"End time: {datetime.now()}")

if __name__ == "__main__":
    main()