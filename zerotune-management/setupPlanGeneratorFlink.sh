#!/bin/bash
selectedTask=0
workerNodeDelim=""
learningNodeDelim=""

masterNode=""
workerNodes=()
learningNodes=()
learningNodesTrainingSets=()
learningNodesMetrics=()
learningNodesDeleteExistings=()
workerNodesAsString=()
learningNodesAsString=()

getMasterNode(){
    read -p "master node IP/hostname (currently: $masterNode) (keep current by empty input / press enter): " masterNodeTemp;
    if [ ! -z $masterNodeTemp ]
    then
        masterNode=$masterNodeTemp
        echo masterNode=$masterNode > masterNode
    fi
}

getWorkerNodes(){
    echo ""
    echo "saved worker nodes: $workerNodesAsString"
    echo "Do you want to use the worker node IPs/hostnames again or remove and set them new?"
    select yn in "Use worker IPs from history" "Remove and set workers"; do
        case $yn in
            "Use worker IPs from history" ) updateWorkers=0; break;;
            "Remove and set workers" ) updateWorkers=1; break;;
        esac
    done
    if [ "$updateWorkers" = 1 ]
    then
        workerNodes=()
        while true ;
        do
            read -p "worker node IP/hostname (end further worker specification by empty input / press enter again): " workerNode;
            if [ -z "$workerNode" ]
            then
                break;
            else
                workerNodes+=($workerNode)
            fi
        done
        updateWorkerNodesAsString
        echo ${workerNodes[@]} > workerNodes
    fi
}

getLearningNodes(){
    echo ""
    echo "saved learning nodes: $learningNodesAsString"
    echo "Do you want to use the learning node IPs/hostnames again or remove and set them new?"
    select yn in "Use learning IPs from history" "Remove and set learning nodes"; do
        case $yn in
            "Use learning IPs from history" ) updateLearningNodes=0; break;;
            "Remove and set learning nodes" ) updateLearningNodes=1; break;;
        esac
    done
    if [ "$updateLearningNodes" = 1 ]
    then
        learningNodes=()
        learningNodesTrainingSets=()
        learningNodesMetrics=()
        learningNodesNoJoins=()
        learningNodesDeleteExistings=()
        echo "end further learning nodes specification by empty input / press enter again"
        while true ;
        do
            read -p "learning node IP/hostname: " learningNode;
            if [ -z "$learningNode" ]
            then
                break;
            else
                read -p "learning node training set: " learningNodeTrainingSet;
                select lt in "Use latency metric" "Use throughput metric"; do
                    case $lt in
                        "Use latency metric" ) learningNodeMetric="latency"; break;;
                        "Use throughput metric" ) learningNodeMetric="throughput"; break;;
                    esac
                done
                echo "Do the queries in the training data contain joins? Without joins, the features needs to be adapted and joinKeyClass gets removed."
                select yn in "With join: Contains queries with joins (Template 2 / Template 3 / Template 1+2+3)" "No join: Only linear query (Template 1)"; do
                    case $yn in
                        "With join: Contains queries with joins (Template 2 / Template 3 / Template 1+2+3)" ) learningNodesNoJoin="false"; break;;
                        "No join: Only linear query (Template 1)" ) learningNodesNoJoin="true"; break;;
                    esac
                done
                echo "Do you want to keep the existing training model or delete it?"
                select yn in "Keep existing training model" "Delete existing training model"; do
                    case $yn in
                        "Keep existing training model" ) learningNodesDeleteExisting="false"; break;;
                        "Delete existing training model" ) learningNodesDeleteExisting="true"; break;;
                    esac
                done
                learningNodes+=($learningNode)
                learningNodeTrainingSets+=($learningNodeTrainingSet)
                learningNodeMetrics+=($learningNodeMetric)
                learningNodesNoJoins+=($learningNodesNoJoin)
                learningNodesDeleteExistings+=($learningNodesDeleteExisting)
            fi
        done
        updateLearningNodesAsString
        echo ${learningNodes[@]} > learningNodes
        echo ${learningNodeTrainingSets[@]} > learningNodeTrainingSets
        echo ${learningNodeMetrics[@]} > learningNodeMetrics
        echo ${learningNodesNoJoins[@]} > learningNodesNoJoins
        echo ${learningNodesDeleteExistings[@]} > learningNodesDeleteExistings
    fi
}

updateWorkerNodesAsString(){
    workerNodesAsString=""
    workerNodeDelim=""
    for w in ${workerNodes[@]}; do
        workerNodesAsString=$workerNodesAsString$workerNodeDelim$w
        workerNodeDelim=","
    done
    
}

updateLearningNodesAsString(){
    learningNodesAsString=""
    learningNodeDelim=""
    for l in ${learningNodes[@]}; do
        learningNodesAsString=$learningNodesAsString$learningNodeDelim$l
        learningNodeDelim=","
    done
    
}

connectToMasterNode(){
    getMasterNode
    ./distributedUploadAndStart.sh -c
}

buildFlinkAndPlanGeneratorFlinkAndDockerImageAndUploadToKubernetes(){
    getMasterNode
    ./build.sh -pf
    ./buildDocker.sh -bu
    ./distributedUploadAndStart.sh -C
    cd kubernetes
    ansible-playbook playbooks/70-install-flink-cluster.yml
}

UploadToKubernetes(){
    getMasterNode
    ./distributedUploadAndStart.sh -CH
    cd kubernetes
    ansible-playbook playbooks/70-install-flink-cluster.yml
    cd ..
    ./distributedUploadAndStart.sh -CH
}

setupKubernetesWithMongoDBBuildAndUploadAndRunFlinkAndPlanGeneratorFlinkOnKubernetes(){
    getMasterNode
    getWorkerNodes
    ./build.sh -pf
    ./buildDocker.sh -bu
    ./distributedUploadAndStart.sh -Hk
}
collectResults(){
    getMasterNode
    ./distributedUploadAndStart.sh -d
}

portForwardFlinkWebUI(){
    getMasterNode
    ./build.sh -e
    ./distributedUploadAndStart.sh -t
}

setupLearningNodes(){
    getLearningNodes
    ./distributedUploadAndStart.sh -Hl
}


callScripts(){
    if [ "$selectedTask" = "1" ] ; then
        ./build.sh -p
    fi
    if [ "$selectedTask" = "2" ] ; then
        ./build.sh -fD
    fi
    if [ "$selectedTask" = "3" ] ; then
        ./build.sh -FD
    fi
    if [ "$selectedTask" = "4" ] ; then
        ./build.sh -pfD
    fi
    if [ "$selectedTask" = "5" ] ; then
        ./build.sh -pfDcmr
    fi
    if [ "$selectedTask" = "6" ] ; then
        setupLearningNodes
    fi
    if [ "$selectedTask" = "8" ] ; then
        connectToMasterNode
    fi
    if [ "$selectedTask" = "9" ] ; then
        buildFlinkAndPlanGeneratorFlinkAndDockerImageAndUploadToKubernetes
    fi
    if [ "$selectedTask" = "10" ] ; then
        UploadToKubernetes
    fi
    if [ "$selectedTask" = "12" ] ; then
        setupKubernetesWithMongoDBBuildAndUploadAndRunFlinkAndPlanGeneratorFlinkOnKubernetes
    fi
    if [ "$selectedTask" = "13" ] ; then
        collectResults
    fi
    if [ "$selectedTask" = "14" ] ; then
        portForwardFlinkWebUI
    fi
    if [ "$selectedTask" = "15" ] ; then
        ./distributedUploadAndStart.sh -z
    fi
}




askParameters(){
    echo "
┌───────────────────────────────────────────────────┐
│▛▀▖▜       ▞▀▖               ▐        ▛▀▘▜ ▗    ▌  │
│▙▄▘▐ ▝▀▖▛▀▖▌▄▖▞▀▖▛▀▖▞▀▖▙▀▖▝▀▖▜▀ ▞▀▖▙▀▖▙▄ ▐ ▄ ▛▀▖▌▗▘│
│▌  ▐ ▞▀▌▌ ▌▌ ▌▛▀ ▌ ▌▛▀ ▌  ▞▀▌▐ ▖▌ ▌▌  ▌  ▐ ▐ ▌ ▌▛▚ │
│▘   ▘▝▀▘▘ ▘▝▀ ▝▀▘▘ ▘▝▀▘▘  ▝▀▘ ▀ ▝▀ ▘  ▘   ▘▀▘▘ ▘▘ ▘│
└───────────────────────────────────────────────────┘
    "
    echo "========= setup PlanGeneratorFlink started ========="
    echo "What do you want to do?"
    select yn in "Build exclusively PlanGeneratorFlink" "Build exclusively Flink (only rebuild flink-streaming-java,flink-clients & flink dist)" "Build exclusively Flink (from scratch)" "Build Flink and PlanGeneratorFlink" "Build Flink and PlanGeneratorFlink, (re)start MongoDB & clean log/db and run local cluster" "Setup learning nodes" "(removed)" "Connect to master node" "Build Flink and PlanGeneratorFlink, build and distribute docker image and upload to Kubernetes, Clean MongoDB" "Upload latest build to Kubernetes, Clean MongoDB" "(removed)" "Setup Kubernetes Cluster with MongoDB, build & build docker image & upload & run Flink and PlanGeneratorFlink on Kubernetes" "Collect PlanGeneratorFlink results to local folder" "Port forward flink web ui" "Get Kubernetes Dashboard Token"; do
        case $yn in
            "Build exclusively PlanGeneratorFlink" ) selectedTask=1; break;;
            "Build exclusively Flink (only rebuild flink-streaming-java,flink-clients & flink dist)" ) selectedTask=2; break;;
            "Build exclusively Flink (from scratch)" ) selectedTask=3; break;;
            "Build Flink and PlanGeneratorFlink" ) selectedTask=4; break;;
            "Build Flink and PlanGeneratorFlink, (re)start MongoDB & clean log/db and run local cluster" ) selectedTask=5; break;;
            "Setup learning nodes" ) selectedTask=6; break;;
            "(removed)" ) break;;
            "Connect to master node" ) selectedTask=8; break;;
            "Build Flink and PlanGeneratorFlink, build and distribute docker image and upload to Kubernetes, Clean MongoDB" ) selectedTask=9; break;;
            "Upload latest build to Kubernetes, Clean MongoDB" ) selectedTask=10; break;;
            "(removed)" ) break;;
            "Setup Kubernetes Cluster with MongoDB, build & build docker image & upload & run Flink and PlanGeneratorFlink on Kubernetes" ) selectedTask=12; break;;
            "Collect PlanGeneratorFlink results to local folder" ) selectedTask=13; break;;
            "Port forward flink web ui" ) selectedTask=14; break;;
            "Get Kubernetes Dashboard Token" ) selectedTask=15; break;;
        esac
    done
    
    echo "========= setup PlanGeneratorFlink finished ========="
}


[[ -f masterNode ]] && source masterNode
[[ -f workerNodes ]] && readarray workerNodes < workerNodes
[[ -f learningNodes ]] && readarray learningNodes < learningNodes
updateWorkerNodesAsString
updateLearningNodesAsString

askParameters
callScripts