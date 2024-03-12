import os

from pygraphviz import DotError
from tqdm import tqdm
import pygraphviz as pgv
import networkx as nx
from sklearn.preprocessing import RobustScaler
import numpy as np
import json

from learning.constants import Feat, Info


def typecast_features(feature_name, feature_values):
    # FLINK FEATURE DEFINITION
    """Do necessarily typecast for the given features"""
    # int encoding
    if feature_name in [Feat.EVENT_RATE, Feat.NUM_DOUBLE, Feat.NUM_STRING,
                        Feat.NUM_INTEGER, Feat.TUPLE_W_IN, Feat.TUPLE_W_OUT,
                        Feat.SLIDING_LENGTH, Feat.WINDOW_LENGTH, Feat.PARALLELISM, 
                        Feat.MAX_CPU_FREQ, Feat.PHYSICAL_CPU_CORES, Feat.TOTAL_MEMORY, 
                        Feat.NETWORK_LINK_SPEED, Feat.COMPONENT]:
        feature_values = [int(i) for i in feature_values]

    # float encoding
    elif feature_name == Feat.SELECTIVITY:
        feature_values = [(float(i)) for i in feature_values]

    # categorical encoding
    elif feature_name in [Info.OP_TYPE, Feat.JOIN_CLASS, Feat.WINDOW_POLICY, Feat.WINDOW_TYPE,
                          Feat.AGG_FUNCTION, Feat.AGG_CLASS, Feat.KEY_BY_CLASS, Feat.FILTER_FUNCTION, Feat.FILTER_CLASS]:
        # do nothing, categorical embedding
        return feature_values

    # no encoding
    elif feature_name in [Info.HOST, Info.ID, Info.LITERAL,
                          Info.INPUT_RATE, Info.OUTPUT_RATE, Info.CPU_LOAD, Feat.INSTANCES]:
        return None
    
    else:
        raise Exception("Feature " + feature_name + " not in features")
    return feature_values


def compute_feature_statistic(feature_values):
    """
    :param feature_values:  A list of values (categorical or numerical) from a specific feature
    where to extract statistics from
    :return: dictionary of feature statistics / embeddings
    """
    if all([isinstance(v, int) or isinstance(v, float) or v is None for v in feature_values]):
        scaler = RobustScaler()
        np_values = np.array(feature_values, dtype=np.float32).reshape(-1, 1)
        scaler.fit(np_values)
        feature_statistic = dict(max=float(np_values.max()),
                                 scale=scaler.scale_.item(),
                                 center=scaler.center_.item(),
                                 type=str("numeric"))
    else:
        unique_values = set(feature_values)
        feature_statistic = dict(value_dict={v: id for id, v in enumerate(unique_values)},
                                 no_vals=len(unique_values),
                                 type=str("categorical"))
    return feature_statistic


def load_or_create_feat_statistics(data_paths):
    """ Writes the statistics file to disk
    :param data_paths: Dictionary that contains related paths
    :return: none
    """
    if os.path.exists(data_paths["stats"]):
        print("Statistics exists at: ", data_paths["stats"])
        with open(data_paths["stats"]) as stat_file:
            feature_statistics = json.load(stat_file)
        return feature_statistics

    all_operators = []  # create a plain list of all operators in the training data
    for g in tqdm(os.listdir(data_paths["graphs"]), desc="Gather feature statistics"):
        try:
            graph = nx.Graph(pgv.AGraph(os.path.join(data_paths["graphs"], g)))
            graph_nodes = list(graph.nodes(data=True))
            for operator in graph_nodes:
                all_operators.append(dict(operator[1]))
        except DotError:
            print("Error while reading: " + g)

    # reformatting list of operators into a dictionary that contains each feature with its corresponding list of values
    feature_dict = dict()
    for operator in tqdm(all_operators, desc="Collecting features from all operators"):
        for key in operator.keys():
            feature_list = feature_dict.get(key)
            if not feature_list:
                feature_list = []
            feature_list.append(operator.get(key))
            feature_dict[key] = feature_list

    # preprocess all features
    statistics_dict = dict()
    for feature_name, feature_values in tqdm(feature_dict.items(), desc="Extract statistics"):
        feature_values = typecast_features(feature_name, feature_values)
        if feature_values:
            statistics_dict[feature_name] = compute_feature_statistic(
                feature_values)

    # save as json
    with open(data_paths["stats"], 'w') as outfile:
        json.dump(statistics_dict, outfile, sort_keys=True)

    return statistics_dict
