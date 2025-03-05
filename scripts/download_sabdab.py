import os
import requests
import concurrent.futures
import time
from typing import List, Dict, Tuple
from datetime import datetime
from tqdm import tqdm

class SabdabDownloader:
    def __init__(self):
        # Create output directories
        self.output_dir = "datasets/raw/sabdab"
        self.ids_dir = "datasets/raw/sabdab_ids"
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.ids_dir, exist_ok=True)

        # Download failure configurations
        self.failed_downloads: Dict[str, DownloadFailure] = {}
        self.MAX_RETRY_ATTEMPTS = 3
        self.RETRY_WAIT_TIME = 300  # 5 minutes before retry
        self.max_workers = 8  # Maximum number of parallel download threads

    def fetch_pdb_ids(self) -> List[str]:
        """Fetch PDB IDs from SAbDab summary file"""
        tsv_file = os.path.join(self.ids_dir, "sabdab_summary_all.tsv")
        
        if not os.path.exists(tsv_file):
            raise FileNotFoundError(f"Summary file not found: {tsv_file}")
        
        pdb_ids = []
        print("Reading SAbDab summary file...")
        
        with open(tsv_file, "r") as f:
            lines = f.readlines()
            for line in lines[1:]:  # Skip header
                columns = line.strip().split("\t")
                pdb_id = columns[0]  # PDB ID is in first column
                pdb_ids.append(pdb_id)
        
        # Remove duplicates
        pdb_ids = list(set(pdb_ids))
        print(f"Found {len(pdb_ids)} unique PDB IDs in SAbDab database")
        return pdb_ids

    def download_structures(self, pdb_ids: List[str]) -> None:
        """Download all SAbDab structure files"""
        print("\nStarting structure downloads...")
        self._process_batch(pdb_ids)

        # Process failed downloads
        if self.failed_downloads:
            print(f"\nProcessing {len(self.failed_downloads)} failed downloads...")
            self._retry_failed_downloads()

        # Output final failure statistics
        self._save_failure_summary()

    def _download_single_file(self, pdb_id: str) -> Tuple[bool, str]:
        """Download a single structure file from SAbDab"""
        output_file = os.path.join(self.output_dir, f"{pdb_id}.pdb")
        
        # Skip if file already exists
        if os.path.exists(output_file):
            return True, f"Already downloaded {pdb_id}.pdb"
        
        url = f"https://opig.stats.ox.ac.uk/webapps/sabdab-sabpred/sabdab/pdb/{pdb_id}"
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                with open(output_file, "w") as f:
                    f.write(response.text)
                return True, f"Successfully downloaded {pdb_id}.pdb"
            else:
                error_msg = f"Failed to download {pdb_id}.pdb, status code: {response.status_code}"
                return False, error_msg
        except Exception as e:
            error_msg = f"Error downloading {pdb_id}.pdb: {str(e)}"
            return False, error_msg

    def _process_batch(self, pdb_ids: List[str]) -> None:
        """Process a batch of PDB IDs for downloading"""
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_pdb = {executor.submit(self._download_single_file, pdb_id): pdb_id 
                           for pdb_id in pdb_ids}
            
            # Use tqdm for progress tracking
            with tqdm(total=len(pdb_ids), desc="Downloading") as pbar:
                for future in concurrent.futures.as_completed(future_to_pdb):
                    pdb_id = future_to_pdb[future]
                    try:
                        success, result = future.result()
                        if not success:
                            self._record_failure(pdb_id, result)
                        pbar.update(1)
                        pbar.set_postfix_str(result)
                    except Exception as e:
                        print(f"Error processing {pdb_id}: {str(e)}")
                        pbar.update(1)

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
                elif os.path.exists(os.path.join(self.output_dir, f"{pdb_id}.pdb")):
                    del self.failed_downloads[pdb_id]

    def _save_failure_summary(self) -> None:
        """Save summary of failed downloads"""
        if not self.failed_downloads:
            return

        print("\nFinal failed downloads summary:")
        for pdb_id, failure in self.failed_downloads.items():
            print(f"- {pdb_id}: Failed {failure.attempt} times. Last error: {failure.error_msg}")
        
        failure_file = os.path.join("datasets", "failed_downloads_sabdab.txt")
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
    print(f"Starting SAbDab data download process at {datetime.now()}")
    
    downloader = SabdabDownloader()
    
    # Step 1: Fetch PDB IDs
    pdb_ids = downloader.fetch_pdb_ids()
    
    # Step 2: Download structure files
    downloader.download_structures(pdb_ids)
    
    end_time = time.time()
    duration = end_time - start_time
    print(f"\nDownload process completed in {duration/3600:.2f} hours")
    print(f"End time: {datetime.now()}")

if __name__ == "__main__":
    main()