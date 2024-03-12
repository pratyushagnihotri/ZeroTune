import argparse

import pandas as pd
import matplotlib.pyplot as plt
from learning.preprocessing.extract_feature_statistics import extract_labels_from_file
import numpy as np


def plot_label_distribution(df):
    log_throughput = True
    log_latency = False
    fig = plt.figure()

    fig.add_subplot(311)
    for i, dff in df.groupby("type"):
        if log_latency:
            plt.hist(np.log(dff['latency']), bins=30, alpha=0.7, label=dff["type"].unique()[0])
            plt.title("Log(Latency)")
        else:
            plt.hist(dff[dff.latency<10000]['latency'], bins=30, alpha=0.7, label=dff["type"].unique()[0])
            plt.title("Latency")
            plt.xlim([0,10000])
    plt.legend()

    fig.add_subplot(312)
    for i, dff in df.groupby("type"):
        if log_throughput:
            plt.hist(np.log(dff['throughput']), bins=30, alpha=0.7, label=dff["type"].unique()[0])
            plt.title("Log Throughput")
            plt.xlim([-4,10])
        else:
            plt.hist(dff['throughput'], alpha=0.7, label=dff["type"].unique()[0])
            plt.xlim([0,10000])
            plt.title("(Not log) Throughput")
    plt.legend()

    fig.add_subplot(313)
    for i, dff in df.groupby("type"):
        plt.hist(dff['counter'], bins=30, alpha=0.7, label=dff["type"].unique()[0])
    plt.xlabel("Num of tuples at the sink")
    plt.yscale("log")
    plt.ylabel("#")
    plt.title("Log Counter")
    plt.xlim(0, 100000)
    plt.legend()
    fig.tight_layout()
    plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset_path', default=None, required=True)
    args = parser.parse_args()
    labels = extract_labels_from_file(args.dataset_path+"/query.labels")
    df = pd.DataFrame.from_dict(labels, orient='index')
    df = df.replace('null', np.nan)
    df = df.astype(dtype={"latency": float, "name": str, "throughput": float, "counter": float, "duration": float})
    df[["cluster", "type", "id"]] = df["name"].str.split("-", expand=True)
    plot_label_distribution(df)