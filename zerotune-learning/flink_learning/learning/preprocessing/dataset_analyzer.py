import os
import pygraphviz as pgv
import networkx as nx
from learning.dataset.dataset_creation import extract_labels_from_file
import pandas as pd
import numpy as np
from tqdm.auto import tqdm

def analyze_graphs(data_path, tp_filter=True):
    tqdm.write("Analyzing " + data_path)
    defect_queries = set()
    graph_path = data_path + "/graphs"
    label_path = data_path + "/query.labels"
    
    for graph in os.listdir(graph_path):
        try:
            g = nx.DiGraph(pgv.AGraph(os.path.join(graph_path, graph))).to_directed()
        except:
            tqdm.write("Error in graph: " + graph)
            defect_queries.add(graph)
        for node in g.nodes(data=True):
            _, node_data = node
            if node_data["operatorType"] in ["SourceOperator"]:
                if len(node_data) != 10:
                    defect_queries.add(graph)
            if node_data["operatorType"] in ["WindowedJoinOperator"]:
                if len(node_data) != 13:
                    defect_queries.add(graph)
            if node_data["operatorType"] in ["FilterOperator"]:
                if len(node_data) != 11:
                    defect_queries.add(graph)
            if node_data["operatorType"] in ["WindowedAggregateOperator"]:
                if len(node_data) != 15:
                    defect_queries.add(graph)
    defect_queries = [q.replace(".graph","") for q in defect_queries]
    tqdm.write(str(len(defect_queries)) + " queries have missing data characteristics ({:4.1f}".format(len(defect_queries)/len(os.listdir(graph_path))*100) + "% )")

    labels = extract_labels_from_file(label_path, False)
    df = pd.DataFrame.from_dict(labels, orient='index')
    df = df.replace('null', np.nan)
    df = df.astype(dtype={"latency": float, "id": str, "throughput": float, "counter": float, "duration": float})
    null_queries = set(df[df.latency.isnull()].index)
    tqdm.write(str(len(null_queries)) + " Queries have a missing label ({:4.1f}".format(len(null_queries)/len(os.listdir(graph_path))*100) + "% )")

    negative_queries = list(df[df.latency < 0].index)
    tqdm.write(str(len(negative_queries)) + " have a negative label ({:4.1f}".format(len(negative_queries)/len(os.listdir(graph_path))*100) + "% )")
    too_short_queries = list(df[df.duration < 30].index)
    tqdm.write(str(len(too_short_queries)) + " have a duration under 30s ({:4.1f}".format(len(too_short_queries)/len(os.listdir(graph_path))*100) + "% )")
    
    low_tp_queries = list()
    if tp_filter:
        low_tp_queries = list(df[df.throughput < 15].index)
        tqdm.write(str(len(low_tp_queries)) + " have a throughput below 15 t/s ({:4.1f}".format(len(low_tp_queries)/len(os.listdir(graph_path))*100) + "% )")
    else:
        print("No throughput filter applied.")

    # outlier_latency_queries = list(df[df.latency > 15000].index)
    # tqdm.write(str(len(outlier_latency_queries)) + " have a very high latency but we are not discarding them ({:4.1f}".format(len(outlier_latency_queries)/len(os.listdir(graph_path))*100) + "% )")
    
    merged = set()
    merged.update(defect_queries)
    merged.update(null_queries)
    merged.update(negative_queries)
    merged.update(too_short_queries)
    merged.update(low_tp_queries)
    #merged.update(outlier_latency_queries)
    query_count = len(os.listdir(graph_path))
    discarded_count = len(merged)
    tqdm.write(str(query_count) + " Queries are existing in dataset")
    tqdm.write(str(discarded_count) + " Queries disqualify ({:4.1f}".format(discarded_count/query_count*100) + "% )")
    tqdm.write(str(query_count-discarded_count) + " queries left for training.")

    # remove defect queries
    for graph in os.listdir(graph_path):
        if graph[:-6] in merged:
            os.remove(os.path.join(graph_path, graph))
    return query_count, discarded_count