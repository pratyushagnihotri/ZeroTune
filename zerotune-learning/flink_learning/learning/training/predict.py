import functools
from operator import itemgetter

from torch.utils.data import DataLoader
from learning.dataset.dataset import GraphDataset
from learning.dataset.dataset_creation import  load_graphs_from_disk, collator
from learning.training.checkpoint import save_csv
from learning.training.train import predict_query_set


def predict(feature_statistics, test_paths, model_name, metric, checkpoint):
    graphs = load_graphs_from_disk(test_paths["graphs"], test_paths["labels"])
    dataset = GraphDataset(graphs)
    batch_size = 32
    dataloader_args = dict(batch_size=batch_size,
                           shuffle=False,
                           collate_fn=functools.partial(
                               collator,
                               feature_statistics=feature_statistics, metric=metric))
    loader = DataLoader(dataset, **dataloader_args)

    csv_stats, epochs_wo_improvement, epoch, model, optimizer, metrics, finished = \
        itemgetter("csv_stats", "epochs_wo_improvement", "epoch", "model", "optimizer", "metrics", "finished") \
            (checkpoint)

    print("Write down predictions for test set")
    predictions = predict_query_set(loader, model, metric)
    save_csv(predictions["query_stats"], test_paths["pred"] + model_name + ".pred.csv")
    return predictions