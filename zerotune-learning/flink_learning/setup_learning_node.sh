#/bin/bash
# This script is used to setup the environment on a cloudlab node

# Update the package list and install python3-pip, mc, and htop packages
sudo apt update && sudo apt -y install python3-pip mc htop

# Create a new directory /mnt/zeroshot
sudo mkdir /mnt/zeroshot

# Use a script to configure additional filesystem space in /mnt/zeroshot
sudo /usr/local/etc/emulab/mkextrafs.pl /mnt/zeroshot

# Get the username of the currently logged in user
currentUser=$(whoami)

# Change the ownership of /mnt/zeroshot directory to the current user
sudo chown -R $currentUser /mnt/zeroshot

# Navigate to the /mnt/zeroshot directory
cd /mnt/zeroshot

# Set git to store credentials for future use
git config --global credential.helper store

# Clone the ZeroTune repository from GitHub
git clone https://github.com/pratyushagnihotri/ZeroTune.git

# Navigate to the specified subdirectory
cd ZeroTune/zerotune-learning/flink_learning

# Setup the graphviz including compliation and installation
tar -xzf graphviz-7.1.0_modded.tar.gz
cd graphviz-7.1.0/
./configure
make
sudo make install
cd ..

# Install the Python packages specified in requirements.txt
pip3 install -r requirements.txt

# Add the current directory to the PYTHONPATH environment variable
export PYTHONPATH=${PYTHONPATH}:$(pwd)

# Install some LaTeX and related packages for visualization
sudo apt-get install dvipng texlive-latex-extra texlive-fonts-recommended cm-super

echo "Next steps:"
echo "Copy training data to folder res. You can also copy multiple folders into a temp folder and use merge_training_data.py to merge them."
echo "Cleanup the training data with python3 learning/preprocessing/dataset_analyzer.ipynb"
echo "Start training!"
echo 
echo 
echo "cd /mnt/ZeroTune/zerotune-learning/flink_learning/"
echo "python3 learning/preprocessing/prepare_experiments.py --experiment_path /mnt/ZeroTune/zerotune-experiments/actual_used_training_sets/train_random_template1 --align_size auto"
echo "python3 main.py --training_data /mnt/ZeroTune/zerotune-experiments/actual_used_training_sets/train_random_template1/training_data --mode train --metric latency --no_join"