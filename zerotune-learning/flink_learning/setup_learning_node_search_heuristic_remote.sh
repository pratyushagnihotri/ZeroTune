#/bin/bash
# This script is used to setup the environment on a cloudlab node for use by the PGF search heuristic

# Update the package database and install python3-pip, which is needed to install Python packages using pip.
sudo apt update && sudo apt -y install python3-pip

# Create a new directory named 'zeroshot' in the '/mnt' directory.
sudo mkdir /mnt/zeroshot

# Use a specific script (presumably from the Emulab network testbed) to configure additional filesystem space at the /mnt/zeroshot location.
sudo /usr/local/etc/emulab/mkextrafs.pl /mnt/zeroshot

# Assign the username of the current user to the 'currentUser' variable.
currentUser=$(whoami)

# Change the ownership of the '/mnt/zeroshot' directory to the current user.
sudo chown -R $currentUser /mnt/zeroshot

# Change the current directory to '/mnt/zeroshot'.
cd /mnt/zeroshot

# Configure Git to store credentials, so it remembers usernames and passwords for repositories.
git config --global credential.helper store

# Clone the 'ZeroTune' repository from GitHub into the current directory.
git clone https://github.com/zerotune/ZeroTune.git

# Navigate into the specified subdirectory within the cloned repository.
cd ZeroTune/zerotune-learning/flink_learning

# Set 'graphviz-7.1.0_modded.tar.gz' tarball, compile and install
tar -xzf graphviz-7.1.0_modded.tar.gz
cd graphviz-7.1.0/
./configure
make
sudo make install

cd ..

# Set the TMPDIR environment variable (for temporary files) to '/mnt/zeroshot' and install Python packages specified in 'requirements.txt'.
TMPDIR=/mnt/zeroshot pip3 install -r requirements.txt

# Add the current directory to the PYTHONPATH environment variable, which determines where Python looks for modules to import.
export PYTHONPATH=${PYTHONPATH}:$(pwd)

# Create a new directory named 'pgf-results' in '/mnt/zeroshot' to store results.
mkdir /mnt/zeroshot/pgf-results