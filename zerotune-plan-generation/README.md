<h1> ZeroTune - Parallel Query Plan Generator Flink </h1>

`zerotune-plan-generation`is a Apache Flink client application to generate parallel query plans for training and testing data to be used for zero-shot learning.


## Getting Started with Parallel Query Generator

1. [Previous step: Setup Cluster](#setup)
1. [Run Arguments](#runArguments)
    - [General arguments](#generalArguments)
    - [Additional arguments](#additionalArguments)
1. [Enumeration strategy](#enumerationStrategy)
1. [Search heuristic](#searchHeuristic)
1. [Reproduce evaluations data](#reproduceEvaluationData)
1. [Next steps: Zero-shot model](https://github.com/pratyushagnihotri/ZeroTune/tree/master/zerotune-learning/flink_learning)

## Setup Cluster<a name="setup"></a>

To setup a cluster and run the client follow the readme from [ZeroTune-Management Setup](https://github.com/pratyushagnihotri/ZeroTune/tree/master/zerotune-management) for detailed setup instruction.

## Run Arguments<a name="runArguments"></a>

There are several run arguments available to e.g. define which data should be generated or which model the search heuristic should use. 
You can use them in the local setup environment as well as in the cluster environment. 
In the following, they are explained.

#### General Arguments<a name="generalArguments"></a>

- `logdir`: Specify where the graph- and label files get stored. The directory is required to exist. 
- `environment`: Defines in which environment PlanGeneratorFlink is running. Can be `kubernetes` for remote cluster execution, `localCluster` for local cluster execution (i.e. started by `setupPlanGeneratorFlink.sh` -> `5) Run local cluster`) or `localCommandLine` (i.e. having a local cluster setup and running PlanGeneratorFlink from the command line like calling `./bin/flink run`)
- `mode`: Defines what training/testing data should be generated. Enter one or multiple out of: `train`, `test`, `randomspikedetection`, `filespikedetection`, `smartgrid` or `advertisement`.
- `numTopos`: Defines how much queries should run per type. For training, a `numTopos` value of 3 would mean you get 3 query runs of template1, 3 of template2 and 3 of template3. For other modes it can be a little bit different. Look up `createAllQueries()` in `AbstractQueryBuilder.java` for insights.

#### Additional Arguments<a name="additionalArguments"></a>

- `debugMode`: In debugMode several things are firmly defined (i.e. seed for randomness or max count of generated source records). Sometimes helpful in the development process.
- `duration`: Duration per query in ms - including warm up time. Can be necessary to adapt for large cluster queries because resource creation can take some time. 
- `UseAllOps`: Only relevant for synthetic training data generation (-> mode = train). With this option it is assured, that every possible operator is used and its not random if a filter operator is placed or not. 
- `interceptor`: Defines if and what experiment interceptor should be used. Currently only `eventrate` is available. An interceptor changes query parameter during the data generation. 
- `interceptorStart`: Needs `interceptor` to be set. Defines the starting point of the intercepted query parameter. 
- `interceptorStep`: Needs `interceptor` to be set. Defines the step size of the intercepted query parameter. 
- `sourceParallelism`: With this option you can define the parallelism of the source operators manually. 
- `minParallelism`: Minimum Parallelism to be used. Default: 1
- `maxParallelism`: Maximum Parallelism to be used. Default: Determine max. parallelism automatically (only available on kubernetes cluster)
- `deterministic`: With this option activated, it is secured that no randomness in the query generation is used and instead predefined parameters will define the query. You can define them in `Constants.java`. This option is only available for synthetic training data generation (-> mode = train).
- `templates`: Determines which templates should be executed (Format: "--templates template1,template3" (no space between)). Only usable with synthetic training and testing templates (mode = train and mode = test). 

<hr>

### Enumeration Strategy<a name="enumerationStrategy"></a>
`enumerationStrategy`: Defines the enumeration strategy to be used. Can be `RANDOM`,  `OPTISAMPLE`. 

- `RANDOM`
The random enumeration strategy randomly chooses a parallelism from the given inclusive boundaries i.e. available maximum number of cores in physical node. The random enumeration strategy is also used as the default strategy in Plan Generator Flink.

- `OPTISAMPLE`
Unlike random enumeration strategy, the OPTISAMPLE is a rulebased enumeration strategy uses the charactereistics of workload (query and workload) and physical resources such incoming event rate and operator selectivity, outgoing rate, number of cores to enumerate parallelism of upstream and downstream operators.

<hr>

## Search Heuristic<a name="searchHeuristic"></a>
- `searchHeuristic`: Determine the optimal parallelism set by using a search heuristic approach. Based on an existing query graph file the optimal parallelism set will be determined either by running only zero-shot model predictions, by actually running queries or by predicting queries and run a subset of them. Needs `searchHeuristicFile`, `searchHeuristicZerotuneLearningPath` and `searchHeuristicModelPath`. 
- `searchHeuristicPredictions`: Only in combination with `searchHeuristic`: With this parameter search heuristic will only use predictions. It does not run actual queries as long as `searchHeuristicPredictionComparison` isn't also set.
- `searchHeuristicPredictionComparison`: Only in combination with `searchHeuristic` and `searchHeuristicPredictions`. It will use the predictions after completion to run actual querys of the 100 top, 100 medium and 100 bad predicted queries.
- `searchHeuristicFile`: Only in combination with `searchHeuristic`. Filepath to a file containing the parameters of the search heuristic query. You can use a .graph file from a previous query run. The query is rebuilt using the data in the file. Some things cannot be influenced, e.g. the placement. 
- `searchHeuristicZerotuneLearningPath`: Only in combination with `searchHeuristic`: Location of zerotune-learning implementation, e.g. /home/user/dsps/zerotune-learning/flink_learning. This is needed for preparing the actual query runs to be compared with predictions, as those query runs needs to be cleaned. 
- `searchHeuristicModelPath`: Only in combination with `searchHeuristic`: Location of the zero-shot model which should be used for predictions of the costs for parallelism sets. 
- `searchHeuristicThreshold`: Only in combination with `searchHeuristic`: Defines the threshold for binarySearch search heuristic. If the model is improving below the threshold, search stops. This is not implemented so far. 

### Search Heuristic - Optimizer

With the Search Heuristic the optimal sets of parallelism degrees can be found. For this purpose, an existing .graph file is used as a template and various sets of parallelism degrees are evaluated.
To select the sets of parallelism degrees, the existing enumeration strategies can be used.
There are three different modes in which the search heuristic can be used:
- `Only Predictions`
- `Predictions + Actual Comparison with 100 top, 100 middle, 100 bad predicted queries`
- `Predictions + Actual Comparison of all queries`

The resulting prediction calculation depends: If actual a query runs, it uses the real graph file for prediction (thus including correct selectivity but also varying placement and other factors). If it uses only predictions the graph files are artificially created and keep all values (=placement, selectivity, ...) except the parallelism degree. Therefore they are better comparable but are not 100% realistic, as e.g. selectivity can maybe change depending on the computing power.

### Setup of a Search Heuristic Node

To setup a Search Heuristic Node, additional steps to the normal Kubernetes setup are required:
- Follow the *First time setup remote kubernetes cluster environment* steps from `zerotune-management`. You can skip the first three steps if you already has setup a cluster before.
- Make sure, that the query you want to use as template is placed in `~/dsps/zerotune-plan-generation/res/searchHeuristicQueries` and the model is placed in `~/dsps/zerotune-plan-generation/res/models`
- Run `setup_learning_node_search_heuristic_local.sh` from `zerotune-learning/flink_learning` on your local computer. It will ask for the server name of the master node (node0) and the corresponding username and uploads the graph file, the model and the setup script. 
- To run the setup script which installs the learning environment, connect to the server (you can use `setupPlanGeneratorFlink.sh` -> `8) Connect to master node`) and run the script `setup_learning_node_search_heuristic_remote.sh` in your home directory. This script will ask for credentials of the git repository. 
- Now you can run the Search Heuristic and later collect the results in the `logDir`.

### `Only Predictions` Mode

To only use the zero-shot model and infer the sets of parallelism degrees, use the *Only Predictions* mode.  
This means, that no actual query will be run and the optimal set of parallelism degrees are determined by the zero-shot model only. This is the fastest mode but you don't get any comparison data to evaluate if the determined set of parallelism degrees is actually one of the best.  
No kubernetes cluster is needed for this mode, but often it is still helpful to run it on a server because for a high amount of queries (like 100 000) it can take a few hours and needs some computation power.  
To use this mode, this example *Plan Generator Flink* runtime parameter can be used:
`--logdir mnt/dsps/pgf-results --environment kubernetes --numTopos 100000 --searchHeuristic --searchHeuristicZerotuneLearningPath /mnt/dsps/zerotune-learning/flink_learning --searchHeuristicModelPath ~/flink/models/random-template1+2+3 --searchHeuristicFile ~/flink/searchHeuristicQueries/2_way_join.graph --enumerationStrategy RANDOM --sourceParallelism 10 --searchHeuristicPredictions`

<hr>

## Reproduce Evaluations Data<a name="reproduceEvaluationData"></a>
The steps to reproduce an evaluation end-to-end are explained in [ZeroTune-Management readme](https://github.com/pratyushagnihotri/ZeroTune/tree/master/zerotune-management#readme). In ZeroTune, we have used various training and testing range for data generation and inference to evaluate model performance. Here, we mentioned some example to vary different parameters to generate data from `zerotune-plan-generation`.

### Training Data Generation

**Random enumeration strategy, linear query structure**  
`--mode train --logdir ~/pgf-results --environment kubernetes --numTopos 3000 --templates template1`

**Random enumeration strategy, 2-way join query structure**  
`--mode train --logdir ~/pgf-results --environment kubernetes --numTopos 3000 --templates template2`

**Random enumeration strategy, 3-way join query structure**  
`--mode train --logdir ~/pgf-results --environment kubernetes --numTopos 3000 --templates template3`

**Rule-Based enumeration strategy, linear query structure**  
`--mode train --logdir ~/pgf-results --environment kubernetes --numTopos 3000 --enumerationStrategy OPTISAMPLE --templates template1`

**Rule-Based enumeration strategy, 2-way join query structure**  
`--mode train --logdir ~/pgf-results --environment kubernetes --numTopos 3000 --enumerationStrategy OPTISAMPLE --templates template2`

**Rule-Based enumeration strategy, 3-way join query structure**  
`--mode train --logdir ~/pgf-results --environment kubernetes --numTopos 3000 --enumerationStrategy OPTISAMPLE --templates template3`
  
### Testing Data Generation

 **Cluster Sizes**  
 `--mode train --logdir ~/pgf-results --environment kubernetes --numTopos 3000`

 **Event Rates**  
 `--mode test --logdir ~/pgf-results --environment kubernetes --numTopos 50 --templates testB`

 **Window Durations**  
 `--mode test --logdir ~/pgf-results --environment kubernetes --numTopos 100 --templates testC`

 **Window Lengths**  
 `--mode test --logdir ~/pgf-results --environment kubernetes --numTopos 100 --templates testD`

 **Tuple Widths**  
 `--mode test --logdir ~/pgf-results --environment kubernetes --numTopos 100 --templates testA`

 **Unseen Synthetic Test Queries**  
 `--mode test --logdir ~/pgf-results --environment kubernetes --numTopos 600 --templates testE`

 **Benchmark: Advertisement**  
 `--mode advertisement --logdir ~/pgf-results --environment kubernetes --numTopos 7000`

 **Benchmark: Random Spike Detection**  
 `--mode randomspikedetection --logdir ~/pgf-results --environment kubernetes --numTopos 7000`

 **Benchmark: File Spike Detection**  
 `--mode filespikedetection --logdir ~/pgf-results --environment kubernetes --numTopos 3200`

 **Benchmark: Smartgrid**  
 `--mode smartgrid --logdir ~/pgf-results --environment kubernetes --numTopos 3200`

 **Unseen Hardware**  
 `--mode train --logdir ~/pgf-results --environment kubernetes --numTopos 1500`

<hr>

 ## Tips

 - For working with search heuristic, many queries and therefore a lot of data gets generated. This can lead to hard disk space problems on Cloudlab. Instead, try to use a separate mounted volume like it's explained in the setup description of Search Heuristic.
 
 - If you use the Search Heuristic, the path to clean the results folder has likely changed to /mnt/dsps/pgf-results. You can adapt the setup scripts by changing `resultLogsDirRemotePath` in `distributedUploadAndStart.sh` (L131).