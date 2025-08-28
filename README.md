# GitLab Crawler

A Python-based crawler that collects public events from the main GitLab instance as well as from publicly accessible 
self-hosted GitLab instances.

Developed independently by Sergiu Mocanu as part of the European project [CodeCommons](https://codecommons.org/).
This project demonstrates skills in:
- __Python development__ (modular design, CLI interface)
- __Web crawling & API integration__ (GitLab main + self-hosted instances)
- __Research software engineering__ (software metadata extraction for [Software Heritage](https://www.softwareheritage.org/))

### Features

- Crawls public instances of __GitLab.com__, both main and self-hosted, for public activity
- Stores all the recovered events locally in a __JSON__ format
- Collects and stores data about the forge activity:
  - Number of updated projects
  - Number of recovered events
  - Request response time
- CLI-based configuration for flexible usage
- Compatible with `requirements.txt` for easy environment setup

### Installation

Clone the repository and install dependencies:

    git clone https://github.com/SergiuMocanu10/gitlab-events-crawler
    cd gitlab-events-crawler
    pip install -r requirements.txt

### Usage

Launch the crawler through the CLI:

    python src/gl_get_benchmark.py --help

Example run:
    
    python src/gl_get_benchmark.py --instance gitlab.com --target_dir ~/gitlab_events

> [!NOTE]
> In the current implementation, the events are written at the beginning of a new hour past the crawler launch
> or once the crawler execution is stopped

For a more detailed visualization of the crawler behavior activate the `--verbose` option

### Crawler parallel execution

Additionally, you can execute a benchmark for running the crawler on multiple self-hosted instances of GitLab using
[GNU Parallel](https://www.gnu.org/software/parallel/):

    sudo apt update && sudo apt install -y parallel python3-venv
    cd crawler_parallel_execution
    chmod +x run_benchmark.sh
    ./run_benchmar.sh

The benchmark will crawl the list of self-hosted GitLab instances __gl_instances.csv__ in batches of size 250 for a 
duration of 2h per batch. Due to some instances not responding (e.g., request timeout) the benchmark is executed with a 
timeout of 10 minutes per instance and a delay of 1 minute between two failed requests in order to avoid log flooding 
with repetitive errors.

The execution of the benchmark can be observed in the `logs` folder (defaults to `home/gl-crawler-parallel-execution/`),
where you can find the logs for each individual instance, as well as a file that contains all the instance that stopped 
before the 2h period (e.g., server timeout, forbidden request). This file will list the GitLab instances that fail 
but are handled correctly by the crawler. On the other hand, if an error which is not handled by the crawler 
implementation occurs, the instance will not be listed in the `0_master.txt` file.

A Python script was created in order to automate the analysis of the benchmark execution, which can be launched after 
the end of the benchmark. It will iterate over the instance log files and display the executions that lead to unhandled
errors or instance crawls that were aborted.

    python crawler_execution_check.py --help

> [!NOTE]
> The list of self-hosted GitLab instances is not exhaustive

### Project context

This project was developed as part of the CodeCommons initiative, a European project that aims to augment
Software Heritage, the biggest archive of open-source software which holds nearly __2 Petabytes__ of source code. 
CodeCommons' mission is to add to the archive relevant data regarding the stored software:
- metadata found on software forges (e.g., issues, pull requests, discussion)
- programming language detection
- CVE detection
- code-quality metrics
- design patterns and anti-patterns

All this additional information offers a broader context about created software, which in turn can be used to 
efficiently train _truly_ open-source, code-specialized Large Language Models, as well as serve as valuable data for 
various research subjects, which will push computer science even further.

### Contributing

Issues and pull requests are welcome. Please follow standard practices:

1. Fork the repo
2. Create a feature branch (git checkout -b feature/xyz)
3. Commit changes with clear messages
4. Open a Pull Request