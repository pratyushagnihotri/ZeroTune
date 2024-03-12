import functools
from operator import itemgetter

import numpy as np
import optuna
import torch as th
import time

from tqdm import tqdm

from torch import optim
from torch.utils.data import DataLoader

from learning.dataset.dataset_creation import collator
from learning.training.checkpoint import load_checkpoint, save_checkpoint, save_csv
from learning.training.metrics import RMSE, QError, MAPE
from learning.training.zero_shot_model import ZeroShotModel


def validate_model(val_loader, model, epoch=0, epoch_stats=None, metrics=None, max_epoch_tuples=None,
                   verbose=False, log_all_queries=False):
    model.eval()

    with th.autograd.no_grad():
        val_loss = th.Tensor([0])
        labels = []
        preds = []
        probs = []

        # evaluate test set using model
        test_start_t = time.perf_counter()
        val_num_tuples = 0
        for batch_idx, batch in enumerate(tqdm(val_loader, desc="Validation Loader")):
            if max_epoch_tuples is not None and batch_idx * val_loader.batch_size > max_epoch_tuples:
                break

            graph, label = batch
            val_num_tuples += val_loader.batch_size
            output = model(graph)
            # sum up mean batch losses
            val_loss += model.loss_fxn(output, label).cpu()

            preds.append(output.cpu().numpy().reshape(-1))
            labels.append(label.cpu().numpy().reshape(-1))

        if epoch_stats is not None:
            epoch_stats.update(val_time=time.perf_counter() - test_start_t)
            epoch_stats.update(val_num_tuples=val_num_tuples)
            val_loss = (val_loss.cpu() / len(val_loader)).item()
            print(f'val_loss epoch {epoch}: {val_loss}')
            epoch_stats.update(val_loss=val_loss)

        labels = np.concatenate(labels, axis=0)
        preds = np.concatenate(preds, axis=0)
        if verbose:
            print(f'labels: {labels}')
            print(f'preds: {preds}')
        epoch_stats.update(val_std=np.std(labels))
        if log_all_queries:
            epoch_stats.update(val_labels=[float(f) for f in labels])
            epoch_stats.update(val_preds=[float(f) for f in preds])

        # save best model for every metric
        any_best_metric = False
        if metrics is not None:
            for metric in metrics:
                best_seen = metric.evaluate(metrics_dict=epoch_stats, model=model, labels=labels, preds=preds,
                                            probs=probs)
                if best_seen and metric.early_stopping_metric:
                    any_best_metric = True
                    print(f"New best model for {metric.metric_name}")
    return any_best_metric


def prepare_model(trial, feature_statistics, model_dir, model_name, model_state, no_joins):
    """Prepares a model configuration. Either load them from disk or create a new one."""

    if trial:
        loss_class_name = trial.suggest_categorical("loss", ["MSE", "QLoss"])
        lr = trial.suggest_categorical("lr", [1e-2, 1e-3, 1e-4, 1e-5])
    else:
        lr = 1e-3
        loss_class_name = "QLoss"

    # general parameters for fc-out-model
    fc_out_kwargs = dict(p_dropout=0.1,
                         activation_class_name='LeakyReLU',
                         activation_class_kwargs={},
                         norm_class_name='Identity',
                         norm_class_kwargs={},
                         residual=False,
                         dropout=True,
                         activation=True,
                         inplace=True)

    # parameters for final multiplayer-perceptron
    final_mlp_kwargs = dict(width_factor=1,
                            n_layers=5,
                            loss_class_name=loss_class_name,
                            loss_class_kwargs=dict())

    # parameters for tree-layer MLPs
    tree_layer_kwargs = dict(width_factor=1,
                             n_layers=2)

    # parameters for node-type MLPs
    node_type_kwargs = dict(width_factor=1,
                            n_layers=2,
                            one_hot_embeddings=True,
                            max_emb_dim=32,
                            drop_whole_embeddings=False)

    final_mlp_kwargs.update(**fc_out_kwargs)
    tree_layer_kwargs.update(**fc_out_kwargs)
    node_type_kwargs.update(**fc_out_kwargs)

    # Setting seeds for torch and numpy
    th.manual_seed(42)
    np.random.seed(42)

    tree_layer_name = 'MscnConv'
    output_dim = 1
    hidden_dim = 128

    model = ZeroShotModel(hidden_dim=hidden_dim,  # dimension of hidden vector/state
                          final_mlp_kwargs=final_mlp_kwargs,
                          node_type_kwargs=node_type_kwargs,
                          output_dim=output_dim,
                          feature_statistics=feature_statistics,
                          tree_layer_name=tree_layer_name,
                          tree_layer_kwargs=tree_layer_kwargs,
                          no_joins=no_joins)

    optimizer_kwargs = dict(lr=lr)
    optimizer_class_name = 'AdamW'
    optimizer = optim.__dict__[optimizer_class_name](
        model.parameters(), **optimizer_kwargs)

    metrics = [RMSE(),
               MAPE(),
               QError(percentile=50, early_stopping_metric=True),
               QError(percentile=95),
               QError(percentile=99)]

    csv_stats, epochs_wo_improvement, epoch, model, optimizer, metrics, finished = load_checkpoint(model, model_dir,
                                                                                                   model_name,
                                                                                                   optimizer=optimizer,
                                                                                                   metrics=metrics,
                                                                                                   filetype='.pt')
    if model_state == "best":  # todo?
        early_stop_m = find_early_stopping_metric(metrics)
        print("Reloading best model")
        model.load_state_dict(early_stop_m.best_model)

    checkpoint = dict(csv_stats=csv_stats, epochs_wo_improvement=epochs_wo_improvement, epoch=epoch, model=model,
                      optimizer=optimizer, metrics=metrics, finished=finished)

    return checkpoint


def train(trial, feature_statistics, train_dataset, val_dataset, test_dataset, model_dir, model_name, metric,
          checkpoint):
    """ Trains a given model for a given metric  on the train_dataset while validating on val_dataset"""

    csv_stats, epochs_wo_improvement, epoch, model, optimizer, metrics, finished = \
        itemgetter("csv_stats", "epochs_wo_improvement", "epoch",
                   "model", "optimizer", "metrics", "finished")(checkpoint)

    epochs = 1000
    early_stopping_patience = 100
    max_epoch_tuples = 1000000

    print("Training with: " + str(len(train_dataset)) + " queries")
    print(
        "Dataset sizes: " + "Train: " + str(len(train_dataset)) + ", Test: " + str(len(test_dataset)) + ", Val: " + str(
            len(val_dataset)))

    if trial:
        batch_size = trial.suggest_categorical(
            "batch_size", [32, 64, 128, 256])
    else:  # default values
        batch_size = 32

    dataloader_args = dict(batch_size=batch_size,
                           shuffle=True,
                           collate_fn=functools.partial(
                               collator,
                               feature_statistics=feature_statistics, metric=metric))

    train_loader, val_loader, test_loader = DataLoader(train_dataset, **dataloader_args), \
        DataLoader(val_dataset, **dataloader_args), \
        DataLoader(test_dataset, **dataloader_args)

    print("Training model with name: " + model_name)

    while epoch < epochs:
        print(f"Epoch {epoch}")
        epoch_stats = dict()
        epoch_stats.update(epoch=epoch)
        epoch_start_time = time.perf_counter()
        # try:
        train_epoch(epoch_stats, train_loader, model,
                    optimizer, max_epoch_tuples)
        any_best_metric = validate_model(val_loader, model, epoch=epoch, epoch_stats=epoch_stats, metrics=metrics,
                                         max_epoch_tuples=max_epoch_tuples)
        epoch_stats.update(
            epoch=epoch, epoch_time=time.perf_counter() - epoch_start_time)

        # report to optuna
        if trial is not None:
            intermediate_value = optuna_intermediate_value(metrics)
            epoch_stats['optuna_intermediate_value'] = intermediate_value

            print(f"Reporting epoch_no={epoch}, intermediate_value={intermediate_value} to optuna "
                  f"(Trial {trial.number})")
            trial.report(intermediate_value, epoch)

        # see if we can already stop the training
        stop_early = False
        if not any_best_metric:
            epochs_wo_improvement += 1
            if early_stopping_patience is not None and epochs_wo_improvement > early_stopping_patience:
                stop_early = True
        else:
            epochs_wo_improvement = 0
        if trial is not None and trial.should_prune():
            stop_early = True

        # also set finished to true if this is the last epoch
        if epoch == epochs - 1:
            stop_early = True

        epoch_stats.update(stop_early=stop_early)
        print(f"epochs_wo_improvement: {epochs_wo_improvement}")

        # save stats to file
        csv_stats.append(epoch_stats)

        # save current state of training allowing us to resume if this is stopped
        save_checkpoint(epochs_wo_improvement, epoch, model, optimizer, model_dir,
                        model_name, metrics=metrics, csv_stats=csv_stats, finished=stop_early)
        epoch += 1

        # Handle pruning based on the intermediate value.
        if trial is not None and trial.should_prune():
            raise optuna.TrialPruned()

        if stop_early:
            print(
                f"Early stopping kicked in due to no improvement in {early_stopping_patience} epochs")
            break

    test_stats = dict()  # copy(param_dict)
    early_stop_m = find_early_stopping_metric(metrics)
    print("Reloading best model")
    model.load_state_dict(early_stop_m.best_model)

    print("Evaluate test set with best checkpoint")
    validate_model(test_loader, model, epoch=epoch,
                   epoch_stats=test_stats, metrics=metrics, log_all_queries=True)

    print("Write down predictions for test set")
    save_csv(predict_query_set(test_loader, model, metric)["query_stats"],
             model_dir + "/pred-" + model_name + ".csv")

    return optuna_intermediate_value(metrics)


def find_early_stopping_metric(metrics):
    potential_metrics = [m for m in metrics if m.early_stopping_metric]
    assert len(potential_metrics) == 1
    early_stopping_metric = potential_metrics[0]
    return early_stopping_metric


def optuna_intermediate_value(metrics):
    for m in metrics:
        if m.early_stopping_metric:
            assert isinstance(m, QError)
            return m.best_seen_value
    raise ValueError('Metric invalid')


def predict_query_set(loader, model, metric):
    """Predict for a set of queries that is loaded by the loader. Write to disk"""
    model.eval()
    print("Evaluating unseen test dataset")
    query_stats = []
    all_labels = []
    all_outputs = []
    with th.no_grad():
        for batch_idx, batch in enumerate(loader):
            graph, label = batch
            names = graph.names
            output = model(graph)
            label = label.detach().cpu().numpy()
            output = output.detach().cpu().numpy()

            if metric["log"]:
                output = np.exp(output)
                label = np.exp(label)
            all_labels += [*label]
            all_outputs += [*output]
            for idx, query in enumerate(names):
                results = dict(name=query)
                results["label"] = label[idx][0]
                results["pred"] = output[idx][0]
                results["qerror"] = QError().evaluate_metric(
                    label[idx], output[idx])
                query_stats.append(results)

    # compute error metrics on whole test dataset
    median = QError().evaluate_metric(all_labels, all_outputs)
    percentile = QError(percentile=95).evaluate_metric(all_labels, all_outputs)
    full = QError(percentile=99).evaluate_metric(all_labels, all_outputs)
    print("QError (normal scale) -- median: " + str(median) + " -- 95percentile: " + str(percentile) + "-- 99percentile: " + str(
        full))
    return {"query_stats": query_stats, "fifty": median, "ninetyfive": percentile, "ninetynine": full}


def train_epoch(epoch_stats, train_loader, model, optimizer, max_epoch_tuples):
    """Train a single epoch with the train_loader. Store stats in epoch_stats"""
    model.train()
    # run remaining batches
    train_start_t = time.perf_counter()
    losses = []
    errs = []
    for batch_idx, batch in enumerate(tqdm(train_loader, desc='Train Loader')):
        if max_epoch_tuples is not None and batch_idx * train_loader.batch_size > max_epoch_tuples:
            break
        graph, label = batch
        optimizer.zero_grad()
        output = model(graph)
        loss = model.loss_fxn(output, label)
        if th.isnan(loss):
            raise ValueError('Loss was NaN')
        loss.backward()
        optimizer.step()

        loss = loss.detach().cpu().numpy()
        output = output.detach().cpu().numpy().reshape(-1)
        label = label.detach().cpu().numpy().reshape(-1)
        errs = np.concatenate((errs, output - label))
        losses.append(loss)
    mean_loss = np.mean(losses)
    mean_rmse = np.sqrt(np.mean(np.square(errs)))
    epoch_stats.update(train_time=time.perf_counter() -
                       train_start_t, mean_loss=mean_loss, mean_rmse=mean_rmse)
