# GitLab crawler parallel execution

The present bash script executes the GitLab crawler in parallel on all the instances found in the __gl_instances.csv__
file (`gitlab.com` was removed from the CSV due to its much higher activity). The parallel execution uses the __GNU__ 
*parallel*, which can be installed with the __*\_install.sh__ script.

In its current configuration, the instances are executed in batches of 250 `-j 250`, for a duration of 2h 
`--timeout 7200`. Based on previous tests, some of the instances do not respond (e.g., request timeout), therefore the
benchmark is executed with a request timeout of 10 minutes `--timeout 10` and a delay of 1 minute between two failed 
requests `--delay 60`, in order to avoid log flooding with repetitive errors.

The execution of the benchmark can be observed in the *logs* folder (defaults to `home/gl-crawler-parallel-execution/`),
where the logs for each individual instance are written, as well as a file that contains all the instance that stopped 
before the __2h__ period (e.g., server timeout, forbidden request). This file will list the GitLab instances that fail 
but are handled correctly by the crawler. On the other hand, if an error which is not handled by the crawler 
implementation occurs, the instance will not be listed in the `0_master.txt` file.

In order to automate the observation of the parallel benchmark execution, the `crawler_execution_check.py` can be 
launched after the end of the benchmark. It will iterate over the instance log files and display the executions that 
lead to unhandled errors or instance crawls that were aborted.