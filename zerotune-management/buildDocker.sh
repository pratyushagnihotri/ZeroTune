#!/bin/bash

show_help(){
  echo "Usage: $0 [-d] [-b] [-u] [-r] [-h]"
  echo "  -d   Define which directory should be used. It needs to have the subfolders '"$plangeneratorflinkDirName"' and '"$flinkDirName"'. Default is currently '"$directory"'"
  echo "  -b   Build Docker image of Flink build"
  echo "  -u   upload docker image to private registry"
  echo "  -r   repository name used for the private registry (default is '$privateRegistryRepoName')"
  echo "  -h   Show this help"
  echo ""
  echo "Example: $0 -pfkd ~/my-zerotune-project"
  exit 1
}


buildDockerImage(){
    cd $directory
    rm -rf $directory"/flink-docker"
    git clone -b $flinkDockerBranchName https://github.com/apache/flink-docker.git
    cd $directory"/"$flinkDirName"/flink-dist/target/"$flinkBuildVersionName"-bin"
    tar czf $dockerImageName.tgz $flinkBuildVersionName
    mv $dockerImageName.tgz $directory"/flink-docker"
    cd $directory"/flink-docker"
    docker stop flinkimagehoster
    docker rm flinkimagehoster
    docker network rm flinkimagehoster-network
    docker network create flinkimagehoster-network
    docker run -it -d --network flinkimagehoster-network --name "flinkimagehoster" -p 9999:9999 -v `pwd`:/data python:3.7.7-slim-buster python -m http.server 9999
    ./add-custom.sh -u http://flinkimagehoster:9999/data/$dockerImageName.tgz -n $dockerImageName -j 11
    cd $directory"/flink-docker/dev/"$dockerImageName"-ubuntu"
    DOCKER_BUILDKIT=0 docker build --network flinkimagehoster-network --no-cache -t $dockerImageName .
    docker stop flinkimagehoster
    docker rm flinkimagehoster
}


uploadDockerImage(){
    cd $directory"/flink-docker"
    docker login -u="$privateRegistryUsername" -p=$privateRegistryReadWriteKey
    docker tag $dockerImageName $privateRegistryUsername/$privateRegistryRepoName:$dateTimeTag
    docker tag $dockerImageName $privateRegistryUsername/$privateRegistryRepoName
    docker push $privateRegistryUsername/$privateRegistryRepoName:$dateTimeTag
    docker push $privateRegistryUsername/$privateRegistryRepoName
    cd $directory"/zerotune-management"
    echo "dateTimeTag=$dateTimeTag" > latestDockerImageUploadTag
    ./distributedUploadAndStart.sh -H
}



echo "========= buildDocker started ========="

# Initialize our intern variables
flinkDirName="flink-observation"
dockerImageName="custom-flink-1.16"
flinkBuildVersionName="flink-1.16-PGF-bin"
flinkBuildVersionName="flink-1.16-PGF"
plangeneratorflinkDirName="zerotune-plan-generation"
flinkDockerBranchName="dev-1.16"
dateTimeTag=$(date '+%Y%m%d-%H%M%S')



OPTIND=1         # Reset in case getopts has been used previously in the shell.

# Initialize our customizable variables:
directory="$HOME/ZeroTune"
buildDockerImage=0
uploadDockerImage=0

privateRegistryUsername="DefineThisInYourPGFEnvFile"
privateRegistryReadWriteKey="DefineThisInYourPGFEnvFile"
privateRegistryRepoName="DefineThisInYourPGFEnvFile"

source pgf-env

while getopts "d:burh" opt; do
  case "$opt" in
    h|\?)
      show_help
      exit 0
      ;;
    d)  directory=$OPTARG
      ;;
    b)  buildDockerImage=1
      ;;
    u)  uploadDockerImage=1
      ;;
    r)  privateRegistryRepoName=$OPTARG
      ;;
  esac
done

shift $((OPTIND-1))

[ "${1:-}" = "--" ] && shift



if [ "$buildDockerImage" = "1" ] ; then
  buildDockerImage
fi


if [ "$uploadDockerImage" = "1" ] ; then
  uploadDockerImage
fi

 
echo -e "\n\n========= buildDocker finished ========="