import statistics
import subprocess
import argparse
from time import time

import psutil
from cmdbench import BenchmarkResults, benchmark_command

COMMANDS_FILE = 'benchmark_commands.txt'


def main() -> None:
    try:
        args = get_args()
        commands = get_commands()
        iters_num = args['i']
        results = []

        for command in commands:
            if command:
                avg_cpu_perc, max_memory_mb = get_cpu_ram_result(command, iters_num)
                exec_sec = get_execution_time(command, iters_num)
                results.append({'command': command,
                                'avg_cpu_perc': avg_cpu_perc,
                                'max_memory_mb': max_memory_mb,
                                'exec_sec': exec_sec})

        show_result(results)
    except Exception as exc:
        print(exc)


def get_cpu_ram_result(command: str, iters_num: int) -> tuple:
    """Measures and returns CPU and RAM usage."""
    benchmark_result = BenchmarkResults()
    new_benchmark_result = benchmark_command(command, iterations_num=iters_num)
    benchmark_result.add_benchmark_result(new_benchmark_result)
    benchmark_result_final = benchmark_result.get_averages()

    max_memory_mb = benchmark_result_final["memory"]["max"] / 1024 / 1024
    cpu_count = psutil.cpu_count()
    avg_cpu_perc = statistics.mean(
        benchmark_result_final['time_series']['cpu_percentages'])
    avg_cpu_perc = avg_cpu_perc / cpu_count

    return avg_cpu_perc, max_memory_mb


def get_execution_time(command: str, iters_num: int) -> float:
    """Measures and returns the execution time in seconds."""
    exec_times = []

    for _ in range(iters_num):
        start_time = time()

        subprocess.run(
            command, shell=True, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL)

        exec_times.append(time() - start_time)

    return statistics.mean(exec_times)


def get_commands() -> list:
    """Reads and returns the list of commands from the file."""
    with open(COMMANDS_FILE) as file:
        commands = file.read().splitlines()
    return commands


def show_result(results: list[dict]) -> None:
    """Prints result to stdout."""
    for result in results:
        print(f"Command: {result['command']}")
        print(f"Execution time: {result['exec_sec']:.2f} sec")
        print(f"CPU (avg): {result['avg_cpu_perc']:.0f} %")
        print(f"Memory (max): {result['max_memory_mb']:.0f} mb")
        print()


def get_args() -> dict:
    """Sets up the arguments settings and returns a dict with arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '-i', type=int, default=1, metavar='<iterations_num>',
        help='number of iterations.')

    return vars(parser.parse_args())


if __name__ == '__main__':
    main()
