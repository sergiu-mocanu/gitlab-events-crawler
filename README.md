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

We recommend using conda __environment__ or `venv` for isolation.

#### Download the project and install the package:
```angular2html
git clone https://github.com/sergiu-mocanu/gitlab-events-crawler
cd gitlab-events-crawler
pip install -e .
```

#### Run the crawler:
```angular2html
gitlab-crawler --help

# Example run
gitlab-crawler --instance gitlab.com --verbose
```

#### To remove the installed package:
```angular2html
pip uninstall gitlab-crawler
```

> [!NOTE]
> In the current implementation, the events are written at the beginning of a new hour or once the crawler execution is stopped.<br>
> Due to the behavior of GitLab's API -- and in order to guarantee that all the events are retrieved starting at the beginning
> of the launch hour -- the events from the first handful of projects will not be recovered.


### PostgreSQL  Setup (Optional)
Besides writing the data locally, the crawler can also store the recovered GitLab events in a PostgreSQL database.<br>
Follow the steps below to install PostgreSQL, create the database, and configure access.


#### Install PostgreSQL:
```angular2html
sudo apt update
sudo apt install postgresql postgresql-contrib
```

#### Create a `.env` file at the project root:
```angular2html
GLCRAWLER_DB_USER=glcrawler
GLCRAWLER_DB_NAME=glcrawler_db
GLCRAWLER_DB_PASSWORD=&lt;your_password&gt;
```
This password will be used to create the PostgreSQL role.

#### Load the environment variables
```angular2html
set -a
source .env
set +a
```

#### Initialize the database:
```angular2html
chmod +x ./scripts/init_db.sh
./scripts/init_db.sh
```

#### Run the crawler with the `database` argument:
```angular2html
gitlab-crawler -db
```

#### Resetting the database
```angular2html
sudo -u postgres dropdb glcrawler_db
sudo -u postgres dropuser glcrawler
```
Then re-run `/scripts/init_db.sh` if needed.<br>

### Crawler parallel execution

Additionally, you can execute a benchmark for running the crawler on multiple self-hosted instances of GitLab using
[GNU Parallel](https://www.gnu.org/software/parallel/):

    sudo apt update && sudo apt install -y parallel python3-venv
    cd crawler_benchmark
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

    python benchmark_results.py --help

> [!NOTE]
> The list of self-hosted GitLab instances is not exhaustive.

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

### License

This project is licensed under the terms of the MIT license.