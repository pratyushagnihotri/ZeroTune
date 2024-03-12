# %%
import os, sys
import pygraphviz as pgv
import networkx as nx
from learning.dataset.dataset_creation import extract_labels_from_file
import pandas as pd
import numpy as np
import json
import re

data_path = "/home/yourusername/ZeroTune/zerotune-learning/flink_learning/res"
graph_path = data_path + "/old_representation_graphs"
graph_output_path = data_path + "/graphs2"

# %% [markdown]
# Import graph files into `graphs` array

# %%
def replace_big_instances(m):
    match = m.group()
    if len(match) > 15000:
        splitindex = len(match) // 2
        # find { before splitindex in match
        splitindex = match.rfind("{", 0, splitindex)
        match1 = match[:splitindex-2]
        match2 = match[splitindex:]
        return match1+"]\", \"instances2\"=\"["+match2+""
    return match


# %%
graphs = {}
regex = re.compile(r"\"instances\"=\"(.*?)}]\"", re.IGNORECASE)
print("It's normal that there will be errors about scanning a quoted string longer than 16384. This is expected and the error messages can't be hidden.")
error_messages = []
for graph in os.listdir(graph_path):
    try:
        g = nx.DiGraph(pgv.AGraph(os.path.join(graph_path, graph))).to_directed()
        graphs.update({graph: g})
    except:
        try:
            f = open(os.path.join(graph_path, graph), "r")
            file = f.read()
            file = regex.sub(replace_big_instances, file)
            file = file.replace("\n", "")
            file = file.replace("\t", "")
            g = nx.DiGraph(pgv.AGraph(file)).to_directed()
            graphs.update({graph: g})
        except:
            error_messages.append("Error in graph (splitting instances doesn't help): " + graph)

print("Number of graphs: " + str(len(graphs)))

# %% [markdown]
# add physical nodes as separate nodes (`operatorType`: `PhysicalNode`) to graph

# %%
regex = re.compile(r"([a-zA-Z]*)=(.*?)([,}])", re.IGNORECASE)
for graph_name, g in graphs.items():
    physical_nodes = {}
    # for every node in the graph
    edges_to_create = {}
    for node in g.nodes(data=True):
        _, node_data = node
        # only if instances attribute is present
        if "instances" in node_data:
            instances = node_data["instances"]
            if "instances2" in node_data:
                instances = instances[:-1] + ", " + node_data["instances2"][1:]
            # convert Java ArrayList to JSON
            instances = regex.sub(r'"\1":"\2"\3', instances)
            instances = json.loads(instances)
            # for every instance of the current operator
            for instance in instances:
                host = instance["host"]
                # add physical node to graph if not already present
                if instance["host"] not in physical_nodes:
                    physical_nodes.update({host: {"operatorType": "PhysicalNode", "host": host, "maxCPUFreq": instance["maxCPUFreq"],
                                                  "totalMemory": instance["totalMemory"], "networkLinkSpeed": instance["networkLinkSpeed"], "physicalCPUCores": instance["physicalCPUCores"]}})
                # remove unnecessary attributes
                instance.pop("maxCPUFreq")
                instance.pop("totalMemory")
                instance.pop("networkLinkSpeed")
                instance.pop("physicalCPUCores")
                # update occurences of physical node to create edges later
                edge_name = node[0]+"->"+host
                if edge_name in edges_to_create:
                    edge_to_create = edges_to_create[edge_name]
                    edge_to_create["occurences"] = edge_to_create["occurences"]+1
                else:
                    edges_to_create[edge_name]= {"operator": node[0], "host": host, "occurences": 1}
            # update instances attribute in graph
            if len(instances) > 16000:
                raise Exception("Too many instances in graph: " + graph_name)
            node_data["instances"] = instances
    # add physical nodes to graph
    g.add_nodes_from(physical_nodes.items())
    # add edges to graph
    for edge in edges_to_create.items():
        g.add_edge(edge[1]["operator"], edge[1]["host"], occurences=edge[1]["occurences"])
    # add edges between physical nodes
    lastPhysicalNode = None
    for node in g.nodes(data=True):
        _, node_data = node
        if node_data["operatorType"] == "PhysicalNode":
            current_host = node_data["host"]
            if lastPhysicalNode is not None:
                g.add_edge(lastPhysicalNode, current_host)
            lastPhysicalNode = current_host


# %%
# write graphs to file
for graph_name, g in graphs.items():
    nx.drawing.nx_agraph.write_dot(g, graph_output_path+"/"+graph_name)


