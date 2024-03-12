#/bin/bash
echo "This script needs to be adapted if you want to use it with a different username or path"

# Prompts the user for the hostname of the server they want to connect to and stores it in the 'server' variable.
read -p "server hostname? " server

# Prompts the user for the username they want to use on the remote server and stores it in the 'username' variable.
read -p "username to use on server? " username

# Uses secure copy (scp) with a specified private key to transfer the 'searchHeuristicQueries' directory to the remote server's '~/flink' directory.
scp -i ~/ZeroTune/zerotune-management/remote_cluster.key -r ~/ZeroTune/zerotune-plan-generation/res/searchHeuristicQueries $username@$server:~/flink

# Uses secure copy (scp) with the same private key to transfer the 'models' directory to the remote server's '~/flink' directory.
scp -i ~/ZeroTune/zerotune-management/remote_cluster.key -r ~/ZeroTune/zerotune-plan-generation/res/models $username@$server:~/flink

# Uses secure copy (scp) with the same private key to transfer the 'setup_learning_node_search_heuristic_remote.sh' script to the home directory of the remote server.
scp -i ~/ZeroTune/zerotune-management/remote_cluster.key -r ~/ZeroTune/zerotune-learning/flink_learning/setup_learning_node_search_heuristic_remote.sh $username@$server:~/setup_learning_node_search_heuristic_remote.sh