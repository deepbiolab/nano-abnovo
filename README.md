# **Nano-AbNovo: Constrained Preference Optimization for Antibody Design**

**Nano-AbNovo** is a lightweight framework for reproducing the core concepts from the paper *"AbNovo: Constrained Preference Optimization for Multi-objective Antibody Design"*. This project implements a diffusion-based model for antibody design, combined with constrained preference optimization (CPO) to meet multi-objective biophysical property requirements.


## **Features**
- **Antibody Design with Diffusion Models**: Joint design of antibody structures and sequences using diffusion generative models.
- **Constrained Preference Optimization (CPO)**: Optimizes binding affinity while explicitly constraining key biophysical properties such as non-specific binding, self-association, and stability.
- **Primal-Dual Optimization**: Dynamically adjusts reward and constraint weights to improve training stability.
- **Structure-aware Protein Language Model**: Leverages large-scale protein structural data to mitigate overfitting caused by limited antibody-antigen training data.
- **Reproducible Results**: Includes code and configurations to reproduce the main experiments from the paper.

> *Working in progress....*

## **Citations**

```
@inproceedings{
ren2025multiobjective,
title={Multi-objective antibody design with constrained preference optimization},
author={Milong Ren and ZaiKai He and Haicang Zhang},
booktitle={The Thirteenth International Conference on Learning Representations},
year={2025},
url={https://openreview.net/forum?id=4ktJJBvvUd}
}
```

## **Acknowledgements**
This project is inspired by the following works:
- [DiffAb: Denoising Diffusion for Antibody Design](https://www.biorxiv.org/content/10.1101/2022.07.10.499510v5.full.pdf)
- [AbX: Score-based Diffusion for Antibody Design](https://openreview.net/pdf?id=1YsQI04KaN)
- [ABDPO: Direct Preference Optimization for Antibody Binding](https://arxiv.org/html/2403.16576v1)


## **License**
This project is licensed under the MIT License.