import pandas as pd
import argparse


def evaluate(testdata_paths):
    for path in testdata_paths:
        print("\n", path)
        predictions = pd.DataFrame()
        for m in ["latency", "throughput"]:
            prediction_path = path + "/predictions/model-" + m + ".pred.csv"
            predictions[["name", m + "_real", m + "_pred", m + "_qerror"]] = pd.read_csv(prediction_path, dtype={'name': object, 'label': float, 'pred': float})

        predictions["name"] = predictions.name.str.replace("node.-", "", regex=True)
        #predictions["name"] = predictions.name.str.replace("node.0-", "", regex=True)
        predictions["test"] = predictions.name.str.split("-", expand=True)[0]
        predictions["name"] = predictions.name.str.split("-", n=1, expand=True)[1]

        if any(path.endswith(i) for i in ["spikedetection", "advertisement-join"]):
            print("Median TPT: ", predictions.median().throughput_qerror)
            print("95th   TPT: ", predictions.quantile(0.95).throughput_qerror)
            print("Median LAT: ", predictions.median().latency_qerror)
            print("95th   TPT: ", predictions.quantile(0.95).latency_qerror)

        elif any(path.endswith(i) for i in ["smartgrid", "advertisement"]):
            predictions[["name", "id"]] = predictions.name.str.split("-", expand=True)
            print("Median TPT: ", predictions.groupby("name").median().throughput_qerror)
            print("95th   TPT: ", predictions.groupby("name").quantile(0.95).throughput_qerror)
            print("Median LAT: ", predictions.groupby("name").median().latency_qerror)
            print("95th   TPT: ", predictions.groupby("name").quantile(0.95).latency_qerror)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset_paths', default=None, nargs='+', required=True)
    args = parser.parse_args()
    evaluate(args.dataset_paths)
