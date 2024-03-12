import os

import dgl
import networkx as nx
import torch as th
from pygraphviz import DotError
from tqdm import tqdm
import numpy as np
import pygraphviz as pgv
from sklearn.preprocessing import RobustScaler
import json

from learning.constants import Feat, Info
from learning.dataset.dataset import GraphDataset
from learning.training.zero_shot_model import MyPassDirection

import matplotlib.pyplot as plt


def load_graphs_from_disk(source_path, label_path):
    """Loading graph paths from the source_path and labels from the label-file into a common dictionary
    :param: source_path: Path where to find all graphs
    :param: label_path: Path where labels are stored
    :return: List of dicts that contain each the graph to a path, the graph name and its labels"""
    labels = extract_labels_from_file(label_path)
    graphs = []  # create a plain list of all operators in the training data
    empty_labels = 0
    for graph_path in tqdm(os.listdir(source_path), desc="Load graphs"):
        graph_name = graph_path[:-6]
        try:
            label = labels[graph_name]
            if "null" in label.values():
                empty_labels += 1
            else:
                graph_path = os.path.join(source_path, graph_path)
                graphs.append(dict(graph_path=graph_path, graph_name=graph_name, label=label))
        except KeyError:
            empty_labels += 1

    print("Graphs with missing labels, that are skipped: " + str(empty_labels))
    return graphs


def load_or_create_datasets(data_paths, metric):
    """ Storing dataset to disk is preferred if working with a fixed dataset.
    In this case, the train/test/val-data are written on the disk in order to ensure that datasets are the same even for
    multiple training runs that we have in optimization.
    Split is 0.9, 0.1, 0.1 for train, test, val.
    Test data are stored to disk to be equal every time, while validation and training data are split every time.
    for every run
    :return: train_dataset, val_dataset, test_dataset
    """
    os.makedirs(data_paths["dataset"], exist_ok=True)

    if "train-" + metric + ".npy" in os.listdir(data_paths["dataset"]):
        print("Datasets exists at: ", data_paths["dataset"])
        train_val_dataset = np.load(data_paths["dataset"] + "/train-" + metric + ".npy", allow_pickle=True)
        test_dataset = np.load(data_paths["dataset"] + "/test-" + metric + ".npy", allow_pickle=True)

    else:
        print("Create datasets")
        # train and validation graphs
        graphs = load_graphs_from_disk(data_paths["graphs"], data_paths["labels"])
        no_graphs = len(graphs)
        graph_indxs = list(range(no_graphs))
        np.random.shuffle(graph_indxs)
        split0 = int(no_graphs * (1 - 0.1))  # training data
        train_val_dataset = GraphDataset([graphs[i] for i in graph_indxs[:split0]])
        test_dataset = GraphDataset([graphs[i] for i in graph_indxs[split0 :]])

        np.save(data_paths["dataset"] + "/train-" + metric + ".npy", train_val_dataset, allow_pickle=True)
        np.save(data_paths["dataset"] + "/test-" + metric + ".npy", test_dataset, allow_pickle=True)

    summed_length = len(train_val_dataset) + len(test_dataset)
    split = int(0.1 * summed_length)
    split1 = len(train_val_dataset) - split
    train_dataset, val_dataset = th.utils.data.random_split(train_val_dataset, [split1, split])
    return train_dataset, val_dataset, test_dataset


def extract_labels_from_file(labels_path, use_print=True):
    """
    :param labels_path: The path to the file, where the labels are stored in a dict-style
    :return: dictionary that contain the query name as key and the labels "throughput" and "latency" as values
    """
    double_labels = 0
    label_dict = dict()
    with open(labels_path) as file:
        for line in file:
            label = json.loads(line)
            query_name = label.get("id")
            if query_name in label_dict.keys():
                if not label_dict[query_name] == label:
                    raise RuntimeError("Query " + query_name + " occurred multiple times in the labels with different values")
                else:
                    double_labels += 1
            label_dict[query_name] = label
    if use_print:
        print("Double Labels found: ", double_labels)
    return label_dict


def encode(feature_list, feature_statistics):
    """Encoding a list of features based on the previously computed feature_statistics
    :param: feature_list: A list of features to encode
    :param: feature_statistics: Dictionary that contains feature statistics
    :return: A tensor of encoded features"""

    # FLINK FEATURE DEFINITION
    # Remove features that we do not consider
    for feature in (Info.ID, Info.HOST, Info.LITERAL, Info.OP_TYPE, Feat.INSTANCES,
                    Info.INPUT_RATE, Info.OUTPUT_RATE, "index"):
        if feature in feature_list.keys():
            feature_list.pop(feature)

    # Encode remaining features
    for feature in feature_list.keys():
        value = feature_list[feature]
        if feature_statistics[feature].get('type') == str("numeric"):
            enc_value = feature_statistics[feature]['scaler'].transform(np.array([[value]])).item()
        elif feature_statistics[feature].get('type') == str("categorical"):
            value_dict = feature_statistics[feature]['value_dict']
            enc_value = value_dict[str(value)]
        else:
            raise NotImplementedError
        feature_list[feature] = th.tensor(enc_value)

    # Sort features as they are not always given in the same order
    feature_list = [feature_list[k].clone().detach() for k in sorted(feature_list.keys())]
    return th.tensor(feature_list)


def add_indexes(graph):
    """ add indexes to each operator, counting for every OperatorType and starting from 0.
    This is needed to distinguish the operators in the heterograph"""
    operators_by_type, operator_indexes_by_type = dict(), dict()
    for node_key, node_data in graph.nodes(data=True):
        operator_type = node_data["operatorType"]
        if operator_type not in operators_by_type.keys():
            operators_by_type[operator_type] = []
            operator_indexes_by_type[operator_type] = 0
        node_data["index"] = operator_indexes_by_type[operator_type]
        operator_indexes_by_type[operator_type] += 1
        operators_by_type[operator_type].append(node_data)


def get_data_dict(graph):
    """obtaining data_dict for heterograph, this maintains the graph structure
    by help of previously assigned unique indexes"""
    data_dict = dict()
    for node_key, node_data in graph.nodes(data=True):
        for c in graph.successors(node_key):
            childs_data = graph.nodes.get(c)
            # default edge type
            edge_type = "to"
            if childs_data["operatorType"] == "PhysicalNode":
                # if the child of a operator is a physical node, the edge type is "on"
                edge_type = "on"
                if node_data["operatorType"] == "PhysicalNode" and childs_data["operatorType"] == "PhysicalNode":
                    # if both nodes are physical nodes, the edge type is "between"
                    edge_type = "between"
            edge = (node_data["operatorType"], edge_type, childs_data["operatorType"])
            if edge not in data_dict.keys():
                data_dict[edge] = ([], [])
            sources, targets = data_dict[edge]
            sources.append(node_data["index"])
            targets.append(childs_data["index"])
            data_dict[edge] = (sources, targets)
    # convert to tensors
    for key in data_dict.keys():
        data_dict[key] = (th.tensor(data_dict[key][0]), th.tensor(data_dict[key][1]))
    return data_dict


def get_feature_dict(graph, feature_statistics):
    """ do the encoding of features, store in feature_dict"""
    feature_dict = dict()
    for node_key, node_data in graph.nodes(data=True):
        operator_type = node_data["operatorType"]
        if not operator_type in feature_dict.keys():
            feature_dict[operator_type] = []
        feature_dict[operator_type].append(encode(node_data, feature_statistics))
    # list of features (tensors) have to be brought into one common tensor
    for feature in feature_dict.keys():
        try:
            feature_dict[feature] = th.stack(feature_dict[feature])
            # if program fails here, the amount of features within the same operator possibly differs
        except RuntimeError:
            print("the features for: " + feature + " in "+ node_key + " cannot be stacked, there are probably features missing.")
    return feature_dict


def get_graph_data(graph):
    """Compute the top node and the index of the top node of a given graph"""
    top_nodes = 0
    top_node, top_node_idx = None, None
    for node_key, node_data in graph.nodes(data=True):
        child = []
        for c in graph.successors(node_key):
            child.append(c)
        # if len(child) > 1:
        #     raise RuntimeError("more than one child for operator " + node_data["id"])
        if len(child) == 0:
            top_nodes += 1
            top_node = node_data["operatorType"]
            top_node_idx = node_data["index"]
    assert top_nodes == 1
    return top_node, top_node_idx


def collator(batch, feature_statistics, metric):
    """Collating the graphs and labels of a batch to a heterograph and a label tensor
    See: https://docs.dgl.ai/en/0.6.x/generated/dgl.heterograph.html"""
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
        label = th.tensor(label_values)
        labels.append(label)

        # preparing graph
        try:
            graph = nx.DiGraph(pgv.AGraph(g["graph_path"]))#.to_directed()
            # debug plot
            # plt.figure(figsize=(13, 10))
            # nx.draw(graph, with_labels=True, arrowsize=20, node_size=8000, width=2)
            # plt.savefig('plotgraph.png', dpi=300)
            # plt.close()

        except DotError:
            raise UserWarning("Could not read file correctly: ", g["graph_path"])
        add_indexes(graph)

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
                if edge[0] == "SourceOperator":
                    canonical_etypes.insert(0, edge)
                else:
                    canonical_etypes.append(edge)

    # use edge set to create the pass directions
    pass_directions = []
    pass_directions_on = []
    pass_directions_to = []
    pass_directions_between = []

    for edge in canonical_etypes:
        if edge[1] == "on":
            pd = MyPassDirection(model_name="operator_on_physical_model", e_name=edge[1], n_src=edge[0], n_dest=edge[2])
            pass_directions_on.append(pd)
        elif edge[1] == "to":
            pd = MyPassDirection(model_name="operator_model", e_name=edge[1], n_src=edge[0], n_dest=edge[2])
            pass_directions_to.append(pd)
        elif edge[1] == "between":
            pd = MyPassDirection(model_name="physical_between_physical_model", e_name=edge[1], n_src=edge[0], n_dest=edge[2])
            pass_directions_between.append(pd)

    [pass_directions.append(pd) for pd in pass_directions_on]
    [pass_directions.append(pd) for pd in pass_directions_to]
    [pass_directions.append(pd) for pd in pass_directions_between]
        

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
