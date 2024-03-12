from datetime import datetime

import optuna
from learning.dataset.dataset_creation import load_or_create_datasets
from learning.preprocessing.extract_feature_statistics import load_or_create_feat_statistics
from learning.training.predict import predict
from learning.training.train import prepare_model, train
from learning.constants import CLI, Metrics
import warnings
import argparse

warnings.filterwarnings("ignore")


def optimize_function(trial, model_name, feature_statistics, train_paths, train_dataset, val_dataset, metric, no_joins):
    """This function is iteratively called by optuna to find the optimal hyperparameter configuration
    """
    model_name = model_name + "-" + str(datetime.now()).replace(" ", "-")
    checkpoint = prepare_model(
        trial, feature_statistics, train_paths["model_dir"], model_name, "current", no_joins)
    return train(trial, feature_statistics, train_dataset, val_dataset, test_dataset, train_paths["model_dir"],
                 model_name, metric, checkpoint)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--training_data', default=None, required=True)
    parser.add_argument('--test_data', default=None, required=False)
    parser.add_argument('--optimize', default=False)
    parser.add_argument('--no_joins', default=False, action='store_true')
    parser.add_argument('--mode', required=True, choices=[CLI.TRAIN, CLI.TEST])
    parser.add_argument('--metric', required=True,
                        choices=[Metrics.LAT, Metrics.TPT])
    args = parser.parse_args()

    # In case of tests, load paths for the test data
    if args.mode == "test":
        if not args.test_data:
            raise ValueError(
                "In test mode, please specify additional test set via `test_data`")
        test_paths = dict(graphs=args.test_data + "/graphs/",
                          pred=args.test_data + "/predictions/",
                          labels=args.test_data + "/query.labels")

    # Define training paths
    train_paths = dict(graphs=args.training_data + "/graphs/",
                       labels=args.training_data + "/query.labels",
                       stats=args.training_data + "/statistics-" + args.metric + ".json",
                       model_dir=args.training_data + "/models/model",
                       dataset=args.training_data + "/datasets/")

    # Define model-name as: model-throughput or model-latency
    model_name = "model" + "-" + args.metric
    metric = dict(type=args.metric, log=False)

    # Create feature statistics and write to file or load statistics from file
    # Feature statistics are later needed to scale and encode the data for training
    feature_statistics = load_or_create_feat_statistics(train_paths)

    if args.mode == "train":
        # create datasets
        # Train, test and validation dataset are split from the main dataset
        train_dataset, val_dataset, test_dataset = load_or_create_datasets(
            train_paths, args.metric)

        # Experimental: optionally do a hyperparameter search with optuna
        if args.optimize:
            study = optuna.create_study(direction="minimize")
            study.optimize(lambda trial:
                           optimize_function(trial, model_name, feature_statistics,
                                             train_paths, train_dataset, val_dataset, metric, args.no_joins),
                           n_trials=10, timeout=None)
            trial = study.best_trial
            print("Best trial:")
            print("  Value: ", trial.value)
            print("  Params: ")
            for key, value in trial.params.items():
                print("    {}: {}".format(key, value))

        # Execute normal training with standard parameters
        else:
            trial = None
            # load the current model if one exists. If not, create a new one
            checkpoint = prepare_model(
                trial, feature_statistics, train_paths["model_dir"], model_name, model_state="current", no_joins=args.no_joins)
            # Train the model on the given datasets
            train(trial, feature_statistics, train_dataset, val_dataset, test_dataset, train_paths["model_dir"],
                  model_name, metric, checkpoint)

    # Test the model on an unseen test dataset. The obtained predictions are stored on disk
    elif args.mode == "test":
        checkpoint = prepare_model(
            None, feature_statistics, train_paths["model_dir"], model_name, model_state="best", no_joins=args.no_joins)
        predict(feature_statistics, test_paths, model_name, metric, checkpoint)
