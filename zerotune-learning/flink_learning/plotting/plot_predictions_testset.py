import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import argparse

plt.rcParams["axes.prop_cycle"] = plt.cycler("color", plt.cm.Dark2.colors)
metrics = {"latency": {"lower": 0.5, "upper": 4},
           "throughput": {"lower": -2, "upper": 4}}


def plot_predictions(prediction_path, target_path, metric):
    df = pd.read_csv(prediction_path, dtype={'name': object, 'label': float, 'pred': float})
    df[["cluster", "template", "query"]] = df["name"].str.split("-", expand=True)
    df["name"] = df.name.str.replace("node.-", "", regex=True)
    fig = plt.figure()
    ax = fig.add_subplot(111)

    x = np.logspace(metrics[metric]["lower"], metrics[metric]["upper"], num=1000)
    Y, X = np.meshgrid(x, x)
    maxf = np.zeros(shape=Y.shape)
    maxf.fill(-9999.99)
    for i, x_ in enumerate(x):
        for j, y_ in enumerate(x):
            maxf[i, j] = max((x_ / y_), (y_ / x_))
    plt.contourf(X, Y, maxf, np.arange(1, 5, 0.25), cmap='Greys')
    ax.axhline(0, color='black', linewidth=0.6)
    ax.axvline(0, color='black', linewidth=0.6)
    cbar = plt.colorbar()
    cbar.ax.set_ylabel('Q-Error', rotation=270, fontsize=12)
    cbar.ax.get_yaxis().labelpad = 15

    for i, dff in df.groupby("template"):
        plt.scatter(dff.label, dff.pred, s=25, edgecolors='none', label=dff["template"].unique()[0], alpha=1)
    plt.legend()

    plt.xlabel("Actual", fontsize=12)
    plt.ylabel("Prediction", fontsize=12)
    lower_lim = np.power(10, float(metrics[metric]["lower"]))
    upper_lim = np.power(10, float(metrics[metric]["upper"]))
    plt.xlim(lower_lim, upper_lim)
    plt.ylim(lower_lim, upper_lim)

    median = round(df["qerror"].median(),2)
    perc = round(df["qerror"].quantile(0.95),2)
    ax.annotate(f"Median:    {median}\n95%-perc: {perc}", xy=(290, 55), xycoords="figure points")
    plt.yscale("log")
    plt.xscale("log")
    plt.title(metric.capitalize() + " - Predictions")
    plt.tight_layout()
    plt.show()

    for i, dff in df.groupby("template"):
        plt.scatter(dff.label, (dff.pred-dff.label) / dff.label, s=25, edgecolors='none', label=dff["template"].unique()[0], alpha=1)
    plt.plot([0, upper_lim], [0, 0], c="black")
    plt.xscale("log")
    plt.xlabel("Actual", fontsize=12)
    plt.ylabel("Deviation", fontsize=12)
    plt.ylim(-2, 2)
    plt.legend()
    plt.title(metric.capitalize() + " - Residuals")
    plt.tight_layout()
    plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset_path', default=None, required=True)
    args = parser.parse_args()

    for metric in ["latency", "throughput"]:
        prediction_path = args.dataset_path + "/models/"+"model/pred-model-"+metric+".csv"
        target_path = args.dataset_path + "/models/"+"model-/pred-model-"+metric+".png"
        plot_predictions(prediction_path, target_path, metric)
