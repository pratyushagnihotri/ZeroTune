import os
import argparse
from tqdm.auto import tqdm
from distutils.dir_util import copy_tree
from learning.preprocessing.dataset_analyzer import analyze_graphs
from learning.preprocessing.merge_training_data import merge

# copy files to intermediate folder, run dataset_analyzer in every experiment,
# merge into output folder

# input parameter is the path to the experiment folder, which contains a folder "src" with all experiments
# assuming following structure in experiment_path:
# - experiment_path
#   - src
#       - experiment_1
#           - query1.graph
#           - query2.graph
#           - query.labels
#       - experiment_2
#           - ...
#       - experiment_3
#           - ...

def create_graph_learning_structure(experiment_path):
    os.makedirs(os.path.join(experiment_path, "graphs"))
    [os.rename(os.path.join(experiment_path, name), os.path.join(experiment_path, "graphs", name))
     for name in os.listdir(experiment_path) if name.endswith(".graph")]


def main(args):
    src_path = os.path.join(args.experiment_path, "src")
    dataset_analyzed_path = os.path.join(
        args.experiment_path, "dataset_analyzed")
    output_path = os.path.join(args.experiment_path, "training_data")

    if os.path.exists(dataset_analyzed_path) and not args.no_analyzer:
        raise Exception(
            "dataset_analyzed_path already exists, please delete it first")
    if os.path.exists(output_path):
        raise Exception("output_path already exists, please delete it first")

    # copy experiment_path folder to dataset_analyzer_path
    if not args.no_analyzer:
        print("⏳ copy src folder, this may take a while...")
        copy_tree(src_path, dataset_analyzed_path)
        print("✅ copy src folder done")

    # run dataset_analyzer in every experiment
    if not args.no_analyzer:
        print("⏳ run dataset_analyzer in every experiment, this may take a while...")
        sub_folders = [name for name in os.listdir(dataset_analyzed_path) if os.path.isdir(
            os.path.join(dataset_analyzed_path, name))]
        all_queries, all_discarded = 0, 0
        for sub_folder in tqdm(sub_folders):
            create_graph_learning_structure(
                os.path.join(dataset_analyzed_path, sub_folder))
            queries, discarded = analyze_graphs(
                os.path.join(dataset_analyzed_path, sub_folder))
            all_queries += queries
            all_discarded += discarded
        print("🧮 " + str(all_queries) + " queries analyzed")
        print("🧮 " + str(all_discarded) + " queries discarded (" +
            str(round(all_discarded / all_queries * 100, 2)) + "%)")
        print("✅ dataset_analyzer done")

    # merge into output folder
    print("⏳ merge into output folder, this may take a while...")
    least_amount_of_queries = False
    if args.align_size != False:
        if args.align_size == "auto":
            if args.no_analyzer:
                least_amount_of_queries = min(len([name for name in os.listdir(os.path.join(src_path, experiment))]) for experiment in os.listdir(src_path))
            else:
                least_amount_of_queries = min(len([name for name in os.listdir(os.path.join(dataset_analyzed_path, experiment, "graphs"))]) for experiment in os.listdir(dataset_analyzed_path))
            print("🔬 Experiment with least data (used as align size): " + str(least_amount_of_queries))
        else:
            least_amount_of_queries = args.align_size
            print("🔬 Align size to be used: " + str(args.align_size))
        if args.no_analyzer:
            merge(src_path, output_path, least_amount_of_queries)
        else:
            merge(dataset_analyzed_path, output_path, least_amount_of_queries)
    else:
        print("🔬 No align size used")
        if args.no_analyzer:
            merge(src_path, output_path)
        else:
            merge(dataset_analyzed_path, output_path)
    print("✅ merge done")
    print("🎉 finished, " + str(len([name for name in os.listdir(
        os.path.join(output_path, "graphs"))])) + " graphs left for training.")



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--experiment_path', default=None, required=True)
    # without analyzer no graph files are deleted based on their characteristics, this can be helpful if you only want to merge the data
    parser.add_argument('--no_analyzer', action='store_true')
    # you can set a maximum size per experiment that should be included in the output folder. You can also use "auto" to determine the size automatically
    # This can be useful for training with equal amount of different experiments (model is fet with equal amount of different experiments)
    parser.add_argument('--align_size', default=None)
    args = parser.parse_args()
    main(args)