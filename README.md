# SIGMOD 2024 Submission (Paper 639): Continual Observation of Joins under Differential Privacy

## Table of Contents
* [About the Project](#about-the-project)
* [Prerequisites](#prerequisites)
    * [Tools](#tools)
    * [Python Dependency](#python-dependency)
    * [Download Data](#download-data)
    * [Create PostgreSQL Database](#create-postgresql-database)
    * [Process Data](#process-data)
* [Implementation](#implementation)
    * [DP Mechanism](#dp-mechanism)
    * [Binary Mechanism](#binary-mechanism)
    * [Clipping Mechanism](#clipping-mechanism)
    * [Residual Sensitiviy](#residual-sensitivity)
    * [Non-Private Setting](#non-private-setting)
* [Demo Collecting Experimental Results](#demo-collecting-experimental-results)
    * [DP Mechanism](#dp-mechanism)
    * [Binary Mechanism](#binary-mechanism)
    * [Clipping Mechanism](#clipping-mechanism)
    * [Residual Sensitiviy](#residual-sensitivity)
## About the Project
This project is to demo our SIGMOD 2024 Submission: Continuous graph pattern counting under differential privacy.

The file structure is as below
```
Project
|
└───Boundary
└───Code
└───Data
│   └───Graph
│   └───TPCH
└───Experiment
└───Query
└───Script
└───Temp

```
`./Boundary` stores the boundaries used in the experiments.

`./Code` stores the codes.

`./Data` stores original Graph and TPCH datasets.

`./Experiment` stores the experimental result.

`./Query` stores the queries in the experiemnts of the paper that contains: 2-line path, 3-line path, triangle, 3-star, 4-star, TPC-H q7, and TPC-H q9 counting queries respectively.

`./Script` stores the scripts used in the experiments of the paper.

`./Temp` stores temporary files generates by programs.
## Prerequisites
### Tools
Before running the project, please install below tools
* [PostgreSQL](https://www.postgresql.org/)
* [Python3](https://www.python.org/download/releases/3.0/)

### Python Dependency
Here are dependencies used in python programs:
* `matplotlib`
* `numpy`
* `sys`
* `multiprocessing`
* `os`
* `math`
* `psycopg2`
* `argparser`

If installing `psycopg2` fails you can try installing `psycopg2-binary`.

### Download Data
Download two data packages ([Graph](https://drive.google.com/file/d/1L6Ho3LtBXewvHydOKUgCpTHSc0H3m_cH/view?usp=share_link) and [TPCH](https://drive.google.com/file/d/1MUGg2hgGhkSCmstHmUwp6FUAdc2oyGCE/view?usp=sharing)) and place the unzipped individual data files in the `./Data/Graph` and `./Data/TPCH` folder respectively.

### Create PostgreSQL Database
To create an empty PostgreSQL databse, for example, named "roadNet-CA", run
```
create database "roadNet-CA";
```
Here, we need seven databases for the graph dataset: `roadNet-CA`, `dimacs9-USA`, `wikipedia_link_nl`, `wikipedia_link_ca`, `dblp_coauthor`, `flickr`, `sx-stackoverflow`. For TPCH database, create database `TPCH`.

### Process Data
#### Import and Clean Data
To import/clean graph data for experimments, go to `./Script` and run `ProcessGraphData.py` to create re-indexed datasets and intermediate folders, which has two parameters
- `-d`: the name of graph dataset;
- `-m`: the option of importing(0)/cleaning(1) data;

For example, to import graph dataset `roadNet-CA`, run
```sh
python ProcessGraphData.py -d roadNet-CA -m 0
```
For example, To import/clean TPCH dataset, go to `./Script` and run `ProcessTPCHData.py`, which has only one parameter
- `-m`: the option of importing(0)/cleaning(1) data;

To clean TPCH database, run
```sh
python ProcessTPCHData.py -m 1
```
#### Delete Top Nodes
To enhance the utility, the dataset used in the experiment removed the node that contributes the most to a portion of the graph data. After import data, go to `./Script` and run `DeleteTopNodes.py` to get the ultimate datasets used in the experiment. There are two parameters.
- `-d`: the name of graph dataset;
- `-r`: the ratio of deleted nodes out of all nodes;

For example, we delete top `5%` nodes with the hightest degree from the original dataset `roadNet-CA`, run
```sh
python DeleteTopNodes.py -d roadNet-CA -r 0.05
```

## Implementation

### DP Mechanism
To run our DP Mechanism, go to `./Script` run `DPMechanism.py`. There are five parameters.

- `-d`: the name of dataset;
- `-q`: the name of query;
- `-e`: the privacy budget $\epsilon$;
- `-b`: the privacy budget $\beta$, which controls the probability of large error happening;
- `-t`: the parameter $\theta$;

For example, to run `roadNet-CA` dataset with 2-line path counting query, run
```sh
python DPMechanism.py -d roadNet-CA -q two_path -e 4 -b 0.1 -t 1
``` 
The result of the experiment are stored in the `./Temp/roadNet-CA/answer` directory.

### Binary Mechanism
To run baseline BM, go to `./Script` run `BinaryMechanism.py`. There are four parameters.

- `-d`: the name of dataset;
- `-q`: the name of query;
- `-e`: the privacy budget $\epsilon$;
- `-T`: the pre-defined truncation threshold $\tau$;

For example, to run `roadNet-CA` dataset with 2-line path counting query, run
```sh
python BinaryMechanism.py -d roadNet-CA -q two_path -e 4 -T 32768
```
The result of the experiment is stored in the `./Temp/roadNet-CA/answer`

### Clippiinig Mechanism
Clipping Mechanism randomly select truncation threshold from $[2^1,2^2,\cdots, 2^{15}]$. To run baseline CM, go to `./Script` run `ClippingMechanism.py`. There are three parameters.

- `-d`: the name of dataset;
- `-q`: the name of query;
- `-e`: the privacy budget $\epsilon$;

For example, to run `roadNet-CA` dataset with 2-line path counting query, run
```sh
python ClippingMechanism.py -d roadNet-CA -q two_path -e 4
```
The result of the experiment is stored in the `./Temp/roadNet-CA/answer`

### Residual Sensitivity
To run baseline Resdual Sensitivity, go to `./Script` run `ResidualSensitivity.py`. There are four parameters.

- `-d`: the name of dataset;
- `-q`: the name of query;
- `-e`: the privacy budget $\epsilon$;
- `-D`: the RS parameter $\delta$;

For example, to run `roadNet-CA` dataset with 2-line path counting query, run
```sh
python ResidualSenstivity.py -d roadNet-CA -q two_path -e 4 -D 1e-10
```
The result of the experiment is stored in the `./Temp/roadNet-CA/answer`

### Non-Private Setting
In order to compute the statistics, we also provide methods for computing the true query results under a non-privacy mechanism.

To obtain real data on the query results, go to the `./Script` folder to run `Truth.py`, which has two parameters

- `-d`: the name of dataset;
- `-q`: the name of query;

For example, to run `roadNet-CA` dataset with 2-line path counting query, run
```sh
python Truth.py -d roadNet-CA -q two_path
```
Move the result of the experiment stored in the `./Temp/roadNet-CA/answer` to `./Experiment/roadNet-CA/Truth/two_path/`. Since the statistics depend on this result, please make sure that the real result of the corresponding dataset query is placed in the correct file path mentioned above before using the script to calculate the statistics.

## Demo Collecting Experimental Results
Notice that, for our DP mechanism, baseline kind of Binary Mechanism and Residual Sensitivity, we repeated the experiment 20 times, and for CM we performed 100 experiments to elimate the randomness.

### DP Mechanism
We implement DP Mechanism for both graph pattern counting queries and TPCH queries. To get the results of the `Ours` in the paper, you can go directly to the `./Script` folder to run `CollectResultsDP.py`, which has two parameters

- `-d`: the name of dataset;
- `-q`: the name of query;

For example,

```sh
python CollectResultsDP.py -d roadNet-CA -q two_path
```
or
```sh
python CollectResultsDP.py -d TPCH -q q7
```

After obtaining the txt files contain the experimental data, move the data in the `./Temp/dataset/answer` directory to `./Experiment/dataset/DP/query_name` (`dataset` and `query_name` are need to be replaced by the names of dataset and query), use `CollectStatDP.py` in `./Script` to get the statistical data, which has two parameters

- `-d`: the name of dataset;
- `-q`: the name of query;

For example, we collect the results for `roadNet-CA` under query `two_path` with

```sh
python CollectStatDP.py -d roadNet-CA -q two_path
```

### Binary Mechanism
Similarly with DP mechanism, to get the result of the `BM` in the paper, go to `./Script` folder to run `CollectResultBM.py` which has two parameters

- `-d`: the name of dataset;
- `-q`: the name of query;

For example,

```sh
python CollectResultsBM.py -d roadNet-CA -q two_path
```

After obtaining the txt files contain the experimental data, move the data in the `./Temp/dataset/answer` directory to `./Experiment/dataset/BM/query_name` (`dataset` and `query_name` are need to be replaced by the names of dataset and query), use `CollectStatBM.py` in `./Script` to get the statistical data, which has two parameters

- `-d`: the name of dataset;
- `-q`: the name of query;

For example, we collect the statistical results for `roadNet-CA` under query `two_path` with

```sh
python CollectStatBM.py -d roadNet-CA -q two_path
```

### Clipping Mechanism
Similarly with BM mechanism, to get the result of the `CM` in the paper, go to `./Script` folder to run `CollectResultCM.py` which has two parameters

- `-d`: the name of dataset;
- `-q`: the name of query;

For example,

```sh
python CollectResultsCM.py -d roadNet-CA -q two_path
```

After obtaining the txt files contain the experimental data, move the data in the `./Temp/dataset/answer` directory to `./Experiment/dataset/CM/query_name` (`dataset` and `query_name` are need to be replaced by the names of dataset and query), use `CollectStatCM.py` in `./Script` to get the statistical data, which has two parameters

- `-d`: the name of dataset
- `-q`: the name of query

For example, we collect the statistical results for `roadNet-CA` under query `two_path` with

```sh
python CollectStatCM.py -d roadNet-CA -q two_path
```

### Residual Sensitivity
Similarly with CM mechanism, to get the result of the `RS` in the paper, go to `./Script` folder to run `CollectResultRS.py` which has two parameters

- `-d`: the name of dataset;
- `-q`: the name of query;

For example,

```sh
python CollectResultsRS.py -d roadNet-CA -q two_path
```

After obtaining the txt files contain the experimental data, move the data in the `./Temp/dataset/answer` directory to `./Experiment/dataset/RS/query_name` (`dataset` and `query_name` are need to be replaced by the names of dataset and query), use `CollectStatRS.py` in `./Script` to get the statistical data, which has two parameters

- `-d`: the name of dataset;
- `-q`: the name of query;

For example, we collect the statistical results for `roadNet-CA` under query `two_path` with

```sh
python CollectStatRS.py -d roadNet-CA -q two_path
```