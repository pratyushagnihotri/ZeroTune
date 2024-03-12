#!/bin/bash

show_help(){
  echo "Usage: $0 [-m] [-w] [-c] [-H] [-k] [-t] [-z] [-d] [-C] [-l] [-h]"
  echo "  -c   Create a ssh session to connect to the master node"
  echo "  -H   Create hosts file for ansible scripts"
  echo "  -k   Install, set up and run kubernetes cluster with preparations for PlanGeneratorFlink"
  echo "  -t   Port forwarding the flink web ui from port :8081 to localhost:80"
  echo "  -z   Print Kubernetes Dashboard Access Token"
  echo "  -d   download PlanGeneratorFlink results to local folder"
  echo "  -C   clean MongoDB database"
  echo "  -l   setup learning environment"
  echo "  -h   Show this help"
  echo ""
  echo "Example: $0 -kfR"
  exit 1
}

runKubernetesInstallation(){
  echo -e "\n\n********* RUN KUBERNETES INSTALLATION *********\n\n"
  cd kubernetes
  ansible-playbook setup-cluster.yml
  cd ..
}

createNewHostsFile(){
  echo -e "\n\n********* CREATE HOSTS FILE *********\n\n"
  # create ansible hosts file
  rm kubernetes/hosts > /dev/null 2>&1
  echo "[masters]" >> kubernetes/hosts
  echo "master ansible_host="$masterNode >> kubernetes/hosts
  echo "" >> kubernetes/hosts
  echo "[workers]" >> kubernetes/hosts
  i=0
  for workerNode in "${workerNodes[@]}"
    do
    if [ -z "$workerNode" ]
    then
      break
    fi
    echo "worker$i ansible_host=$workerNode" >> kubernetes/hosts
    i=$((i+1))
  done
  echo "" >> kubernetes/hosts
  echo "[all:vars]" >> kubernetes/hosts
  echo "dockerFlinkVersion=$dateTimeTag" >> kubernetes/hosts
  echo "username=$usernameNodes" >> kubernetes/hosts
  echo "dockerImage=$dockerImage" >> kubernetes/hosts
  authCredentials=$(echo -n "$privateRegistryUsername:$privateRegistryReadOnlyKey" | base64)
  dockerHubSecret=$(echo '{"auths":{"https://index.docker.io/v1/":{"auth":"'$authCredentials'"}}}' | base64 -w 0)
  echo "dockerHubSecret=$dockerHubSecret" >> kubernetes/hosts
  echo "" >> kubernetes/hosts
  echo "[learning]" >> kubernetes/hosts
  i=0
  for learningNode in "${learningNodes[@]}"
    do
    if [ -z "$learningNode" ]
    then
      break
    fi
    echo "learner$i ansible_host=$learningNode training_set=${learningNodeTrainingSets[$i]} training_metric=${learningNodeMetrics[$i]} training_no_joins=${learningNodesNoJoins[$i]} training_delete_existing=${learningNodesDeleteExistings[$i]}" >> kubernetes/hosts
    i=$((i+1))
  done
  # echo $learningNodesTrainingSets
  echo "" >> kubernetes/hosts
  echo "[learning:vars]" >> kubernetes/hosts
  echo "githubUsername=$githubUsername" >> kubernetes/hosts
  echo "githubAccessToken=$githubAccessToken" >> kubernetes/hosts
  # create ansible config file
  rm kubernetes/ansible.cfg > /dev/null 2>&1
  echo "[defaults]" >> kubernetes/ansible.cfg
  echo "host_key_checking = False" >> kubernetes/ansible.cfg
  echo "inventory = ./hosts" >> kubernetes/ansible.cfg
  echo "remote_user = $usernameNodes" >> kubernetes/ansible.cfg
  echo "private_key_file = $keyPath" >> kubernetes/ansible.cfg
  echo "ansible_python_interpreter = auto" >> kubernetes/ansible.cfg
  echo "" >> kubernetes/ansible.cfg
  echo "[ssh_connection]" >> kubernetes/ansible.cfg
  echo "retries=10" >> kubernetes/ansible.cfg
}

downloadPlanGeneratorFlinkResults(){
  echo -e "\n\n********* COLLECT PLANGENERATORFLINK RESULTS AND DOWNLOAD TO LOCAL FOLDER *********\n\n"
  downloadFolderName=$directory/$managementFolderDirName/results/$(date '+%Y%m%d-%H%M%S')
  mkdir -p $downloadFolderName
  echo -e "The local download folder: $downloadFolderName";
  rsync -rv -e "ssh -o StrictHostKeyChecking=no -i $keyPath" --include '*/' "$usernameNodes@$masterNode:$resultLogsDirRemotePath/" $downloadFolderName
}

portForwardingFlinkWebUI(){
  echo -e "\n\n********* START PORT FORWARDING OF FLINK WEB UI ON :8081 *********\n\n"
  ssh -o "StrictHostKeyChecking no" -i $keyPath -L 8081:127.0.0.1:8081  $usernameNodes@$masterNode "pgrep kubectl | xargs kill > /dev/null 2>&1; kubectl port-forward service/plangeneratorflink-cluster-rest 8081:8081 -n plangeneratorflink-namespace"
}

createSSHConnectionToMasterNode(){
  echo -e "\n\n********* CREATE SSH CONNECTION TO MASTER NODE *********\n\n"
  ssh -o "StrictHostKeyChecking no" -i $keyPath $usernameNodes@$masterNode
}

printKubernetesDashboardAccessTokenToConsole(){
  echo -e "\n\n********* PRINT KUBERNETES DASHBOARD ACCESS TOKEN *********\n\n"
  echo -e "\n\n"
  ssh -o "StrictHostKeyChecking no" -i $keyPath $usernameNodes@$masterNode "kubectl -n kubernetes-dashboard create token admin-user"
  echo -e "\n\n"
  echo "Open Kubernetes Dashboard: https://"$masterNode":32000"
}

cleanMongoDBDatabaseAndResultsFolder(){
  echo -e "\n\n********* CLEAN MONGODB AND RESULTS FOLDER *********\n\n"
  echo "get clusterIP of mongoDBNode: $mongoDBNode"
  mongoDBClusterIP=$(ssh -o "StrictHostKeyChecking no" -i $keyPath $usernameNodes@$masterNode "kubectl get services -n plangeneratorflink-namespace mongodb --no-headers -o custom-columns=':spec.clusterIPs' | grep -oP '[^\[\]].*[^\[\]]'")
  # echo "install mongodb-client on master node"
  # ssh -o "StrictHostKeyChecking no" -i $keyPath $usernameNodes@$masterNode "sudo apt install mongodb-clients -y"
  echo "delete entries in mongoDB ($mongoDBClusterIP)"
  ssh -o "StrictHostKeyChecking no" -i $keyPath $usernameNodes@$masterNode "mongo $mongoDBClusterIP:27017/plangeneratorflink -u $mongoDBPGFUsername -p $mongoDBPGFPassword --eval 'db.query_labels.remove({});db.query_grouping.remove({});db.query_observations.remove({});db.query_placement.remove({});'"
  echo "delete content of result folder on master node"
  ssh -o "StrictHostKeyChecking no" -i $keyPath $usernameNodes@$masterNode "rm -r $resultLogsDirRemotePath/*"
}

setupLearning(){
  echo -e "\n\n********* SETUP LEARNING NODES *********\n\n"
  cd kubernetes
  ansible-playbook playbooks/80-setup-learning-environment.yml
  cd ..
}

echo "========= distributedUploadAndStart started ========="

# Initialize our intern variables
managementFolderDirName="zerotune-management"
resultLogsDirRemotePath="~/pgf-results"
dateTimeTag="latest"
mongoDBNode="node1"
mongoDBPGFUsername="pgf"
mongoDBPGFPassword="pwd"



OPTIND=1         # Reset in case getopts has been used previously in the shell.

# Initialize our customizable variables:
directory="$HOME/ZeroTune"
masterNode=""
workerNodes=""
learningNodes=""
learningNodeTrainingSets=""
learningNodesNoJoins=""
learningNodesDeleteExistings=""
learningNodeMetrics=""
usernameNodes="DefineThisInYourPGFEnvFile"
createSSHConnection=0
createHostsFile=0
installKubernetes=0
portForwarding=0
printKubernetesDashboardAccessToken=0
downloadResults=0
cleanMongoDB=0
learning=0
keyPath=$directory/$managementFolderDirName/"remote_cluster.key"
privateRegistryUsername="DefineThisInYourPGFEnvFile"
privateRegistryReadWriteKey="DefineThisInYourPGFEnvFile"
privateRegistryEmail="DefineThisInYourPGFEnvFile"
privateRegistryReadOnlyKey="DefineThisInYourPGFEnvFile"
githubUsername="DefineThisInYourPGFEnvFile"
githubAccessToken="DefineThisInYourPGFEnvFile"

[[ -f masterNode ]] && source masterNode
[[ -f workerNodes ]] && read -a workerNodes < workerNodes
[[ -f learningNodes ]] && read -a learningNodes < learningNodes
[[ -f learningNodeTrainingSets ]] && read -a learningNodeTrainingSets < learningNodeTrainingSets
[[ -f learningNodesNoJoins ]] && read -a learningNodesNoJoins < learningNodesNoJoins
[[ -f learningNodesDeleteExistings ]] && read -a learningNodesDeleteExistings < learningNodesDeleteExistings
[[ -f learningNodeMetrics ]] && read -a learningNodeMetrics < learningNodeMetrics
[[ -f latestDockerImageUploadTag ]] && source latestDockerImageUploadTag
[[ -f pgf-env ]] && source pgf-env

dockerImage="$privateRegistryUsername/$privateRegistryRepoName"


while getopts "kcHtzdClh" opt; do
  case "$opt" in
    h|\?)
      show_help
      exit 0
      ;;
    c)  createSSHConnection=1
      ;;
    H)  createHostsFile=1
      ;;
    k)  installKubernetes=1
      ;;
    t)  portForwarding=1
      ;;
    z)  printKubernetesDashboardAccessToken=1
      ;;
    d)  downloadResults=1
      ;;
    C)  cleanMongoDB=1
      ;;
    l)  learning=1
      ;;
  esac
done

shift $((OPTIND-1))

[ "${1:-}" = "--" ] && shift

if [ "$createHostsFile" = "1" ] ; then
  createNewHostsFile
fi

if [ "$installKubernetes" = "1" ] ; then
  createNewHostsFile
  runKubernetesInstallation
fi

if [ "$cleanMongoDB" = "1" ] ; then
  cleanMongoDBDatabaseAndResultsFolder
fi

if [ "$portForwarding" = "1" ] ; then
  portForwardingFlinkWebUI
fi

if [ "$downloadResults" = "1" ] ; then
  downloadPlanGeneratorFlinkResults
fi

if [ "$createSSHConnection" = "1" ] ; then
  createSSHConnectionToMasterNode
fi

if [ "$printKubernetesDashboardAccessToken" = "1" ] ; then
  printKubernetesDashboardAccessTokenToConsole
fi

if [ "$learning" = "1" ] ; then
  setupLearning
fi

echo -e "\n\n========= distributedUploadAndStart finished ========="