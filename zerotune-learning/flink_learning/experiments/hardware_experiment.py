import functools
import warnings
from operator import itemgetter

import dgl
import networkx as nx
import matplotlib.pyplot as plt
from pygraphviz import DotError
from sklearn.preprocessing import RobustScaler
from torch.utils.data import DataLoader

from models.dataset.constants import Labels
from models.dataset.dataset_creation import load_or_create_datasets, get_data_dict, \
    get_feature_dict, add_indexes, get_graph_data
from models.preprocessing.extract_feature_statistics import load_or_create_feat_statistics
from models.training.checkpoint import save_csv
from models.training.train import prepare_model, predict_query_set, validate_model
import torch as th
import pygraphviz as pgv
import numpy as np
from models.training.zero_shot_model import MyPassDirection
import pandas as pd

warnings.filterwarnings("ignore")
plt.rcParams["axes.prop_cycle"] = plt.cycler("color", plt.cm.Dark2.colors)
plt.rcParams["font.family"] = "cmss10"


def mod_collator(batch, feature_statistics, metric, instance_size):
    # prepare robust encoder for the numerical fields
    for k, v in feature_statistics.items():
        if v.get('type') == str("numeric"):
            scaler = RobustScaler()
            scaler.center_ = v['center']
            scaler.scale_ = v['scale']
            feature_statistics[k]['scaler'] = scaler

    labels, features, names = [], [], []
    graph_data = []
    data_dicts = []

    for g in batch:
        # collecting names
        names.append(g["graph_name"])

        # preparing labels
        label_values = [float(g["label"][metric["type"]])]  
        if metric["log"]:
            label_values = [max(10e-3, value) for value in label_values] 
            label_values = [float(np.log(i)) for i in label_values]

        label = th.tensor(label_values)
        labels.append(label)

        # preparing graph
        try:
            graph = nx.DiGraph(pgv.AGraph(g["graph_path"])).to_directed()
        except DotError:
            raise UserWarning("Could not read file correctly: ", g["graph_path"])
        add_indexes(graph)

        # This is different - mod: Place either small, medium or large instances
        for node_key, node_data in graph.nodes(data=True):
            node_data["instanceSize"] = instance_size
        #print(node_data)

        # obtaining top node and index
        top_node, top_node_index = get_graph_data(graph)
        graph_data.append(dict(top_node=top_node, top_node_index=top_node_index))

        # preparing data_dicts
        data_dict = get_data_dict(graph)
        data_dicts.append(data_dict)

        # preparing feature_dicts
        feature_dict = get_feature_dict(graph, feature_statistics)
        features.append(feature_dict)

    # collecting all edge types from all graphs in a common set
    canonical_etypes = []
    for d in data_dicts:
        for edge in d.keys():
            if edge not in canonical_etypes:
                if edge[0] == "SpoutOperator":      
                    canonical_etypes.insert(0, edge)
                else:
                    canonical_etypes.append(edge)

    # use edge set to create the pass directions
    pass_directions = []
    for edge in canonical_etypes:
        pd = MyPassDirection(model_name="edge", e_name="to", n_src=edge[0], n_dest=edge[2])
        pass_directions.append(pd)

    graphs = []
    # updating data dicts with remaining edge types that are not yet contained
    for data_dict in data_dicts:
        for edge in canonical_etypes:
            if edge not in data_dict.keys():
                data_dict[edge] = ((), ())

        # create hetero_graphs
        hetero_graph = dgl.heterograph(data_dict)
        graphs.append(hetero_graph)

    # merging feature dicts in to a common dict:
    batched_features = dict()
    for feature in features:
        for operator in feature.keys():
            if operator not in batched_features.keys():
                batched_features[operator] = th.tensor(feature[operator])
            else:
                batched_features[operator] = th.concat([batched_features[operator], feature[operator]])

    # batch graphs and labels
    batched_graph = dgl.batch(graphs)
    batched_graph.pd = pass_directions
    batched_graph.names = names
    batched_graph.features = batched_features
    batched_graph.data = graph_data

    labels = th.stack(labels)
    return batched_graph, labels

training_data = "/home/romanh/projects/storm_learning/training_data/2022-02-07"
size = 10000
train_paths = dict(graphs=training_data + "/graphs/",
                   labels=training_data + "/query.labels",
                   stats=training_data + "/statistics.json",
                   model_dir=training_data + "/models/",
                   dataset=training_data + "/datasets/")

for m in ["latency", "throughput"]:
    # select training metric
    if m == "latency":
        metric = dict(type=Labels.LATENCY, log=False)
    elif m == "throughput":
        metric = dict(type=Labels.THROUGHPUT, log=False)
    else:
        raise ValueError()
    model_name = "model" + "-" + metric["type"]

    feature_statistics = load_or_create_feat_statistics(train_paths)
    train_dataset, val_dataset, test_dataset = load_or_create_datasets(train_paths)

    checkpoint = prepare_model(None, feature_statistics, train_paths["model_dir"], model_name, "best", size)
    batch_size = 32


    instance_sizes = ["small", "medium", "xlarge"]
    for instance_size in instance_sizes:
        dataloader_args = dict(batch_size=batch_size,
                               shuffle=False,
                               collate_fn=functools.partial(
                                   mod_collator,
                                   feature_statistics=feature_statistics, metric=metric, instance_size=instance_size))
        loader = DataLoader(test_dataset, **dataloader_args)

        csv_stats, epochs_wo_improvement, epoch, model, optimizer, metrics, finished = \
            itemgetter("csv_stats", "epochs_wo_improvement", "epoch", "model", "optimizer", "metrics", "finished") \
                (checkpoint)

        print("Evaluate test set with best checkpoint")
        validate_model(loader, model, epoch=epoch, epoch_stats=dict(), metrics=metrics, log_all_queries=True)

        print("Write down predictions for test set")

        save_csv(predict_query_set(loader, model, metric), "./"+m+instance_size+".pred.csv")

plt.rcParams.update({"legend.handlelength":1})
fig, axs = plt.subplots(1,2, sharex=True, figsize=(5.5,2))
for m, ax, ylabel in zip(["throughput", "latency"], axs, ["ms", "ev/s"]):
    df_merged = pd.DataFrame()
    for instance_size in instance_sizes:
        path = "./"+m+instance_size+".pred.csv"
        df = pd.read_csv(path)
        df_merged[instance_size] = df["pred"]
    df_merged["name"] = df.name
    df_merged[["cluster", "template", "id"]] = df_merged.name.str.split("-", expand=True)
    means = df_merged.groupby("template").mean().transpose()
    std = df_merged.groupby("template").sem().transpose()
    print(m)
    print(means, std)
    means.rename(index={"small": 1, "medium": 2, "xlarge": 3}, inplace=True)
    ax.errorbar(means.index-0.04, means["template1"]-0.04, yerr=std["template1"], fmt="X", capsize=5,linestyle="--", label="Linear Query")
    ax.errorbar(means.index, means["template2"], yerr=std["template2"], capsize=5, fmt="o", linestyle="--", label="2-way-join")
    ax.errorbar(means.index+0.04, means["template3"]+0.04, yerr=std["template3"], capsize=5, fmt="^", linestyle="--", label="3-way-join")
    for p in ax.patches:
        ax.annotate(str(round(p.get_height())), (p.get_x() * 1.005, p.get_height() * 0.75))
    ax.set_title(m.capitalize())
    ax.set_ylabel(ylabel)

plt.xticks(ticks=[1, 2, 3], rotation=0, labels=["small", "medium", "large"])
axbox = axs[1].get_position()
leg = plt.legend(fontsize="small", title="Query Structure",  fancybox=False, frameon=True, ncol=1, loc="center right",  bbox_to_anchor=[1.01, 0.77], bbox_transform=fig.transFigure)
leg.get_frame().set_alpha(None)
plt.tight_layout()
plt.savefig("./hardware_experiment.pdf", dpi=250)
