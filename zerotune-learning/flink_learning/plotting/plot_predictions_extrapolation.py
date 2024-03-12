import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import argparse

# Pandas and matplotlib-settings
pd.options.mode.chained_assignment = None  # default='warn'
plt.rcParams.update({
              "axes.prop_cycle": plt.cycler("color", plt.cm.Dark2.colors),
              "axes.titley": 1.02,
              "axes.titlepad": 0,
              "font.family": "cmss10",
              "font.size": 24})
markersize = 16
linestyle = "--"
ylims = [0, 5.5]

# Constants
metrics = ["latency_qerror", "throughput_qerror"]
titles = ["Latency", "Throughput"]
templates = ["template1", "template2", "template3"]
tests = ["test1", "test2", "test3_1", "test3_2", "test4"]
#tests = ["testA", "testB", "testC", "testD", "testE"]
methods = ["plot_test_A", "plot_test_B", "plot_test_C", "plot_test_D"]

def median_and_quantiles(dataframe, target, metric):
    medians = dataframe.groupby([target, 'template'])[metric].median().unstack()
    upper_bound = dataframe.groupby([target, 'template'])[metric].quantile(0.95).unstack()
    lower_bound = dataframe.groupby([target, 'template'])[metric].quantile(0.05).unstack()
    return medians, upper_bound, lower_bound


def plot_test_A(pred, axs):
    """ Test A -- Tuple Width Extrapolation """
    pred["tuple_width"] = pred.name.str.split("-", expand=True)[2].astype(int)
    pred["tuple_width"] = pred["tuple_width"] * 3
    pred["template"] = pred.name.str.split("-", expand=True)[0]
    for ax, metric, title in zip((axs[0,0], axs[1,0]), metrics, titles):
        medians, upper_bound, lower_bound = median_and_quantiles(pred, "tuple_width", metric)
        for template, marker, label in zip(templates, ["X", "o", "^"], ["Linear query", "2-way-join", "3-way-join"]):
            if metric == "latency_qerror":
                ax.plot(medians[template], marker=marker, linestyle=linestyle, markersize=markersize)
            else:
                ax.plot(medians[template], marker=marker, linestyle=linestyle, markersize=markersize, label=label)
        ax.set_ylim(ylims)
        ax.set_xlim([0, 31])
        ax.axvspan(3, 15, alpha=0.2, color="grey")
    axs[0, 0].set_title("A\nTuple Width\n(per source)")
    axs[0, 0].set_ylabel("Latency")
    axs[1, 0].set_ylabel("Throughput")
    axs[1, 0].set_xlabel("nr. of values",  fontsize=30)
    axs[1, 0].set_xticks([0, 10, 20, 30])


def plot_test_B(pred, axs):
    """ Test B -- Event Rate Extrapolation"""
    pred["event_rate"] = pred.name.str.split("-", expand=True)[2].astype(int)
    pred["template"] = pred.name.str.split("-", expand=True)[0]
    pred = pred.drop(pred[pred.event_rate == 10000].index)
    for ax, metric, title in zip((axs[0, 1], axs[1, 1]), metrics, titles):
        medians, upper_bound, lower_bound = median_and_quantiles(pred, "event_rate", metric)
        for template, marker in zip(templates, ["X", "o", "^"]):
            ax.plot(medians[template], marker=marker, linestyle=linestyle, markersize=markersize)
        ax.set_ylim(ylims)
        ax.set_xscale("log")
        ax.axvspan(250, 2000, alpha=0.2, color="grey")
        ax.xaxis.set_major_formatter(mticker.ScalarFormatter())
    axs[0, 1].set_xlim(85, 6000)
    axs[1, 1].set_xlim(85, 6000)
    axs[1, 1].set_xticks([100, 1000, 5000])
    axs[0, 1].set_title("B\nEvent Rate\n(per source)")
    axs[1, 1].set_xlabel("ev/sec", fontsize=30)


def plot_test_C(pred, axs):
    """ Test C -- Window Length (time-based) extrapolation """
    pred["window_length"] = pred.name.str.split("-", expand=True)[3].astype(int) / 1000.0
    pred["template"] = pred.name.str.split("-", expand=True)[0]
    pred = pred.drop(pred[pred.window_length > 8].index)
    for ax, metric, title in zip((axs[0, 2], axs[1, 2]), metrics, titles):
        medians, upper_bound, lower_bound = median_and_quantiles(pred, "window_length", metric)
        for template, marker, label in zip(templates, ["X", "o", "^"], ["Linear query", "2-way-join", "3-way-join"]):
            ax.plot(medians[template], marker=marker, linestyle=linestyle, markersize=markersize)
        ax.set_ylim(ylims)
        ax.axvspan(0.25, 3, alpha=0.2, color="grey")
        ax.xaxis.set_major_formatter(mticker.ScalarFormatter())
    axs[0, 2].set_title("C\nWindow Length\n(time)")
    axs[0, 2].set_xticks([0, 2, 4, 6, 8])
    axs[1, 2].set_xlabel("sec", fontsize=30)


def plot_test_D(pred, axs):
    """ Test D -- Window Length (count-based) extrapolation """
    pred["window_length"] = pred.name.str.split("-", expand=True)[3].astype(int)
    pred["template"] = pred.name.str.split("-", expand=True)[0]
    for ax, metric, title in zip((axs[0,3], axs[1,3]), metrics, titles):
        medians, upper_bound, lower_bound = median_and_quantiles(pred, "window_length", metric)
        for template, marker, label in zip(templates, ["X", "o", "^"], ["Linear query", "2-way-join", "3-way-join"]):
            ax.plot(medians[template], marker=marker, linestyle=linestyle, markersize=markersize)
        ax.set_ylim(ylims)
        ax.axvspan(5, 100, alpha=0.2, color="grey")
        ax.xaxis.set_major_formatter(mticker.ScalarFormatter())
    axs[0, 3].set_title("D\nWindow Length\n(count)")
    axs[1, 3].set_xlabel("nr. of tuples", fontsize=30)
    axs[1, 3].set_xticks([0, 100, 200, 300, 400])


def evaluate(testdata_path):
    predictions = pd.DataFrame()
    for m in ["latency", "throughput"]:
        prediction_path = testdata_path + "/predictions/model-" + m + ".pred.csv"
        predictions[["name", m + "_real", m + "_pred", m + "_qerror"]] = pd.read_csv(prediction_path, dtype={'name': object, 'label': float, 'pred': float})
    predictions["name"] = predictions.name.str.replace("node.-", "", regex=True)
    #predictions["name"] = predictions.name.str.replace("node.0-", "", regex=True)
    predictions["test"] = predictions.name.str.split("-", expand=True)[0]
    predictions["name"] = predictions.name.str.split("-", n=1, expand=True)[1]
    predictions["test"] = predictions.test.str.replace("test4..", "test4", regex=True)

    # Test A, B, C, D
    fig, axs = plt.subplots(2, 4, figsize=(16, 6.5), constrained_layout=True, sharex="col")
    for m, t in zip(methods, tests):
        pred = predictions[predictions.test == t]
        if not pred.empty:
            globals()[m](pred, axs)

    fig.legend(fancybox=False, frameon=True, ncol=3, loc="lower center", fontsize=30)
    fig.supylabel("Median Q-Error")
    plt.savefig("extrapol_experiment.pdf", dpi=250)
    plt.show()

    fig, axs = plt.subplots(2,1, figsize=(10,8))
    pred = predictions[predictions.test == "test4"]
    pred["structure"] = pred.name.str.split("-", expand=True)[0]
    for ax, metric, title in zip(axs, metrics, titles):
        for s in pred["structure"].unique():
            median = str(round(pred[pred.structure == s][metric].median(), 2))
            percentile = str(round(pred[pred.structure == s][metric].quantile(0.95), 2))
            ax.hist(pred[pred.structure == s][metric], bins=30, label=[s + " - med: " + median + " percentile: " + percentile])
        ax.set_title(title)
        ax.legend(fontsize=12)
    plt.tight_layout()
    plt.show()

    fig, axs = plt.subplots(2,1, figsize=(6,10))
    for ax, metric, title, lim in zip(axs, ["latency", "throughput"], titles, [4000, 100]):
        for s in pred["structure"].unique():
            tmp = pred[pred.structure == s]
            ax.scatter(tmp[metric+"_real"],  tmp[metric+"_pred"], label=s)
        ax.set_title(title)
        ax.set_xlim([0,lim])
        ax.set_ylim([0,lim])
        ax.plot([0,lim],[0, lim], c="black")
        ax.legend(fontsize=15)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset_path', default=None, required=True)
    args = parser.parse_args()
    evaluate(args.dataset_path)
