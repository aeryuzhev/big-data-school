# Benchmark
Benchmark is a python module for measuring the following perfomance indicators:

* Execution time
* Avearage CPU usage
* Max RAM usage

## Requirements
**Python 3.6** or higher.

This module requires the following python modules:

| Module                                         | Description                                                                                                                                     |
|------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| [cmdbench](https://pypi.org/project/cmdbench/) | A quick and easy benchmarking tool for any command's CPU, memory and disk usage.                                                                |
| [psutil](https://pypi.org/project/psutil/)     | Cross-platform library for retrieving information on running processes and system utilization (CPU, memory, disks, network, sensors) in Python. |

## Installation

```bash
pip install -r requirements.txt
```

## Usage
```bash
benchmark.py [-i <iterations_num>] [--help] 
```

## Params
| Name          | Value            | Description          |
|---------------|------------------|----------------------|
| `--help`      |                  | shows help message   |
| `-i`          | <iterations_num> | number of iterations |


## Example
Fill in the `benchmark_commands.txt` file with the commands you want to benchmark:
```text
python "/home/user/project/task_02_get_movies_(python)/get-movies.py"
python "/home/user/project/task_02_get_movies_(python)/get-movies.py" --N 3
python "/home/user/project/task_03_get_movies_(sql)/client/get-movies.py"
python "/home/user/project/task_03_get_movies_(sql)/client/get-movies.py" --N 3
```

And then, execute the `benchmark.py`:
```bash
python benchmark.py
```

Result example:
```bash
Command: python "/home/user/project/task_02_get_movies_(python)/get-movies.py"
Execution time: 47.19 sec
CPU (avg): 25 %
Memory (max): 60 mb

Command: python "/home/user/project/task_02_get_movies_(python)/get-movies.py" --N 3
Execution time: 46.83 sec
CPU (avg): 25 %
Memory (max): 59 mb

Command: python "/home/user/project/task_03_get_movies_(sql)/client/get-movies.py"
Execution time: 0.98 sec
CPU (avg): 20 %
Memory (max): 62 mb

Command: python "/home/user/project/task_03_get_movies_(sql)/client/get-movies.py" --N 3
Execution time: 0.29 sec
CPU (avg): 20 %
Memory (max): 38 mb
60
```
