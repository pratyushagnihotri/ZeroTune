<h1> ZeroTune - Zero-Shot Cost Model for Cost Prediction </h1>

ZeroTune has learned component - zero-shot cost model that uses a Graph Neural Network to make cost predictions for seen and unseen parallel query plan in Distributed Parallel Stream Processing.

## Getting Started with Zero-shot cost model

1. Previous step
   -  [Setup cluster](https://github.com/pratyushagnihotri/ZeroTune/tree/master/zerotune-management)
   - [Generate training and testing data](https://github.com/pratyushagnihotri/ZeroTune/tree/master/zerotune-plan-generation)
1. [Prerequisites](#prerequisites)
1. [Run Zero-shot model](#runZeroShotModel)
1. [Zero-shot model training and Inference](#zeroshotmodel)
1. [Reproduce evaluations data](#reproduceEvaluationData)
1. [Sample evaluation plotting](#plotting)

## Prerequisites:<a name="prerequisites"></a>
You can set zero-shot model's training and inference environment both `locally` and `remotely` using our [`ZeroTune-Management Setup`](https://github.com/pratyushagnihotri/ZeroTune/tree/master/zerotune-management) and selection `option 6 for Setup learning nodes` or .
1. Install packages from requirements.txt
2. Create training and optionally additional testdata by use of the synthetic plangenerator program for Apache Storm
3. Verify that the test or training folder contain the following parts:
   1. graphs - This directory contains all enriched query graphs
   2. query.labels - these are the training labels that are collected by the plangenerator program
4. Please run the dataset_analyzer.ipynb from ./learning/preprocessing/ at first on the dataset to train/test for. This eliminates queries without a label, missing data characteristics and so on.

## Steps to run Zero-shot model for Parallel Stream Processing<a name="runZeroShotModel"></a>
1. Make sure `zerotune-learning` is in `ZeroTune` folder
1. Make sure `zerotune-experiments` is in `ZeroTune` folder
1. Switch into `zerotune-learning/flink_learing/graphviz-7.1.0` and install `graphivz` by running the following commands (compiling yourself is necessary as the source code is slightly modified, see `graphviz` chapter for details)
   - ./configure
   - make
   - sudo make install
1. Install `python3-pip` by running `sudo apt update && sudo apt -y install python3-pip`
1. Switch into `zerotune-learning/flink_learning`. You can set zero-shot model's training and inference environment both `locally` and `remotely` using 
   - `remote:` [ZeroTune-Management Setup](https://github.com/pratyushagnihotri/ZeroTune/tree/master/zerotune-management) and selection `option 6 for Setup learning nodes`or 
   - `local:` 
      - python3 -m venv zerotune
      - source venv/bin/activate
      - run `pip3 install -r requirements.txt`
1. Copy training data to `zerotune-learning/flink_learning/res` so that the graph files are in a folder called `graphs` and the `query.labels` is in the main folder `res`, e.g.:
   - `zerotune-learning/flink_learning/res`
      - `query.labels`
      - `graphs`
         - `graph1.graph`
         - `graph2.graph`
         - ...

### Why Graphviz
Maybe you are asking why you need to manual compile graphviz instead of installing it by using your package manager? The reason is, that the graph-Objects of our training data can contains strings that are longer than the maximum possible attribute length of the default graphviz build. In that case the error message `Error: syntax error in line X scanning a quoted string (missing endquote? longer than 16384?)` appears. 
To avoid this error and be able to import longer attributes (its about the instances attribute in queries with high parallelism degrees) the source code of `graphviz` was modified. In `lib/cgraph/scan.c` line 397 and 399 the `YY_BUF_SIZE` is changed to `65536`. It can be the case, that even that is not enough. You can increase this value even more and compile again to try a even higher buffer.

### Procedure

1. At first the program will gather the statistics for the individual features which will later be used for the encoding. These are stored at `statistics.json`
2. In the directory `datasets`, two numpy arrays are saved. These hold the graph paths for either the train set (including validation data) and/or the test set.
3. The model is trained either for latency or throughput. Early stopping can be configured.
4. After model training, the final predictions on the unseen testset are stored in `models/predictions`

## Zero-shot Model Training and Inference<a name="zeroshotmodel"></a>

###  Prepare the Data

The process involves refining a batch of generated queries for effective training and testing. Preprocessing steps are necessary to prepare the data properly. This necessitates a series of steps:

1. **Experiment Selection and Aggregation:** Relevant experiments are chosen based on factors like enumeration strategy or hardware used. Data from these experiments is then gathered in a single directory.

1. **Merging Data:** If an experiment was run multiple times due to errors, its graph files are combined into a common folder. Label files are also merged for consistency.

1. **Data Analysis:** Each graph file is checked for accurate operator feature counts. Cases where operators lack runtime data due to execution delays are identified.

1. **Label Validation:** The integrity of label collection is verified to ensure completeness.

1. **Consolidation of Experiment Setups:** All experiment setups are merged.

1. **Standardizing Query Counts:** To avoid bias, the query count per setup is adjusted to match the lowest query count.

1. **Randomization:** Queries are shuffled randomly to achieve a balanced distribution of experiment setups.

These tasks can be automated using the `prepare_experiments.py` script, enhancing accuracy and efficiency in data preparation.

```
 python3 learning/preprocessing/prepare_experiments.py --experiment_path path to raw data
```

### Train the Model
To train the model on a prepared dataset, call:
		
```
python3 main.py --
--training_data training_data/2022-01-04
--mode train
--metric throughput
```
### Inference a model with separate test queries
While the above call just trains and test for the given dataset, the model can be afterwards applied to predict for new and unseen datasets. The plangenerator can generate several test datasets (extrapolation and benchmark sets). To optain predictions for those, please call:
```
python3 main.py --
--training_data /training_data/2022-01-04
--test_data test_data/2022-01-04
--mode test
--metric throughput
```

### Do hyperparameter search
We use `optuna` to optimize for hyper-parameters. This is experimental and can be a very long-running task.
Since this is a long running execution, we recommend to call it via `nohup` and store the logs since it
contains the model performance.
```
python3 main.py --
--training_data training_data/2022-01-04
--mode train
--optimize
```

## Sameple Evaluation Plotting<a name="plotting"></a>
To monitor the training progress and evaluate the predictions,
a bunch of sample plotting scripts are provided in the `plotting` folder.
Note that the plots are usually not stored on disk but directly displayed.

#### Label distribution
To plot the distribution of latency and throughput in any dataset, call:
`python3 plot_label_distribution.py --dataset_path ./training_data/2022-02-07`

#### Testset predictions
To plot the predictions and q-errors for the unseen test set, call:
```python plot_predictions_testset.py --dataset_path ./training_data/2022-02-07```

#### Extrapolation predictions
To plot the predictions for the extrapolation experiment, call:
```python3 plot_predictions_extrapolation.py --dataset_path /test_data/extrapolation```
The predictions have to be obtained before (see above)

#### Benchmark predictions
To compute the median and 95th percentile Q-errors for the benchmarks (e.g. smartgrid), call:
```
python3 plot_predictions_benchmark.py
--dataset_paths 
./test_data/smartgrid 
./test_data/spikedetection 
./test_data/advertisement
./test_data/advertisement-join
```

### Training progress
To show training curve of the GNN, call:
```
python3 plot_training_progress.py 
--dataset_path training_data/2022-02-07 
--metrics latency throughput
```