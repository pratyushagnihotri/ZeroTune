import os
import random
import string
from tqdm import tqdm

def merge(input_path, output_path, align_size=False):
    # create folder if not exists
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    if not os.path.exists(os.path.join(output_path, "graphs")):
        os.makedirs(os.path.join(output_path, "graphs"))

    sub_folders = [name for name in os.listdir(input_path) if os.path.isdir(os.path.join(input_path, name))]
    # copy .graph files in subfolders to output path
    all_file_names = set()
    prefixes = set()
    for sub_folder in tqdm(sub_folders, desc="Folder processing", position=0, leave=False, colour='green'):
        #    tqdm.write("Processing folder " + sub_folder)
        sub_folder_graphs = ""
        if os.path.exists(os.path.join(input_path, sub_folder, "graphs")):
            sub_folder_graphs = os.path.join(input_path, sub_folder, "graphs")
        else:
            sub_folder_graphs = os.path.join(input_path, sub_folder)
        prefix = ""
        prefix_completed = False
        # while loop to avoid duplicate prefixes just in case
        while not prefix_completed:
            sub_files_with_prefix = [prefix+name for name in os.listdir(sub_folder_graphs) if name.endswith(".graph")]
            # check if sub_files contains duplicates
            if len(all_file_names.intersection(set(sub_files_with_prefix))) > 0:
                prefix = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(3)) + "_"
            else:
                prefix_completed = True
                if prefix != "":
                    tqdm.write("Duplicate files found in subfolder " + sub_folder + ". Will use prefix " + prefix + " to distinguish them.")
                    prefixes.add(prefix)
        all_file_names.update(sub_files_with_prefix)
        sub_files_without_prefix = [name for name in os.listdir(sub_folder_graphs) if name.endswith(".graph")]
        # copy sub_files to output_path
        copied_files = 0
        for sub_file in tqdm(sub_files_without_prefix, desc="Copy files", position=1, leave=False, colour='#808080'):
            if(align_size):
                if copied_files >= int(align_size):
                    break
                copied_files += 1
            with open(os.path.join(sub_folder_graphs, sub_file), "r") as f:
                with open(os.path.join(os.path.join(output_path, "graphs"), prefix + sub_file), "w") as f1:
                    for line in f:
                        if prefix != "":
                            # replace sub_file excluding last part ".graph" in line with prefix + sub_file
                            f1.write(line.replace(sub_file[:-6], prefix + sub_file[:-6]))
                        else:
                            f1.write(line)
        # append root query.labels file
        with open(os.path.join(input_path, sub_folder, "query.labels"), "r") as f:
            with open(os.path.join(output_path, "query.labels"), "a") as f1:
                for line in f:
                    if prefix != "":
                        f1.write(line.replace('"id": "', '"id": "' + prefix))
                    else:
                        f1.write(line)