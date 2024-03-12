 #!/bin/bash

show_help(){
  echo "Usage: $0 [-d] [-D] [-p] [-f] [-F] [-r] [-c] [-m] [-e] [-h]"
  echo "  -d   Define which directory should be used. It needs to have the subfolders '"$plangeneratorflinkDirName"' and '"$flinkDirName"'. Default is currently '"$directory"'"
  echo "  -D   Prepare build with the custom debug config file to be able to append a debugger and have multiple task slots"
  echo "  -p   Build Plangeneratorflink and copies the jar file to flink build"
  echo "  -f   Build Flink (only rebuild the modules flink-streaming-java & flink dist)"
  echo "  -F   Build Flink completely"
  echo "  -r   Stop running local cluster and run a new local cluster"
  echo "  -c   Clean log directories and mongoDB collections"
  echo "  -m   Start / Restart local mongoDB"
  echo "  -e   End running local cluster"
  echo "  -h   Show this help"
  echo ""
  echo "Example: $0 -pfkd ~/my-zerotune-project"
  exit 1
}


stop_local_flink_cluster(){
  echo "\n\n********* STOP CURRENTLY RUNNING CLUSTER *********\n\n"
  "$directory"/"$flinkDirName"/build-target/bin/stop-cluster.sh
}


build_flink(){
  echo "\n\n********* BUILD FLINK FROM SOURCE *********\n\n"
  if [ "$1" = "0" ] ; then
    flinkBuildArg=" -pl flink-streaming-java,flink-clients,flink-dist"
  fi
  cd "$directory"/"$flinkDirName"
  ./mvnw clean install -DskipTests$flinkBuildArg
  rc=$?
  if [ $rc -ne 0 ] ; then
    echo Could not perform flink build, exit code [$rc]; exit $rc
  fi
  cd ..
}


build_plangeneratorflink(){
  echo "\n\n********* BUILD PLANGENERATORFLINK *********\n\n"
  cd "$directory"/"$plangeneratorflinkDirName"
  mvn clean install
  rc=$?
  if [ $rc -ne 0 ] ; then
    echo Could not perform plangeneratorflink build, exit code [$rc]; exit $rc
  fi
  cd ..
}


copy_plangeneratorflink_to_flink_lib(){
  echo "\n\n********* COPY PLANGENERATORFLINK TO FLINK BUILD *********\n\n"
  cp "$directory"/"$plangeneratorflinkDirName"/target/plangeneratorflink*.jar "$directory"/"$flinkDirName"/build-target/lib
}


copy_custom_config_to_flink_build(){
  echo "\n\n********* COPY CUSTOM CONFIG TO FLINK BUILD *********\n\n"
    echo '### ZeroTune start
      env.java.opts.taskmanager: "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005 -Dlog4j.debug"
      ### ZeroTune end' >> "$directory"/"$flinkDirName"/build-target/conf/flink-conf.yaml

      sed -i 's/.*taskmanager.numberOfTaskSlots: 1.*/taskmanager.numberOfTaskSlots: 30/' "$directory"/"$flinkDirName"/build-target/conf/flink-conf.yaml
      sed -i 's/.*rest.bind-address: localhost.*/rest.bind-address: 0.0.0.0/' "$directory"/"$flinkDirName"/build-target/conf/flink-conf.yaml
      sed -i 's/.*taskmanager.memory.process.size: 1728m.*/taskmanager.memory.process.size: 3500m/' "$directory"/"$flinkDirName"/build-target/conf/flink-conf.yaml
      sed -i 's/.*# taskmanager.memory.network.fraction: 0.1*/taskmanager.memory.network.fraction: 0.3/' "$directory"/"$flinkDirName"/build-target/conf/flink-conf.yaml
}

clean_log_dir_and_mongoDB(){
  echo "\n\n********* CLEAN LOCAL LOG DIR AND MONGODB COLLECTIONS *********\n\n"
  rm -rf "$directory"/"$flinkDirName"/build-target/log/*
  rm -rf "$directory"/"$plangeneratorflinkDirName"/logs/*
  echo "if mongo cannot be found in the next step, take care that mongoDB-clients package is installed"
  #mongoDBIP="$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mongodblocal_mongo_1)"
  #mongo $mongoDBIP:27017/plangeneratorflink -u pgf -p pwd --eval 'db.query_labels.remove({});db.query_grouping.remove({});db.query_observations.remove({});db.query_placement.remove({});'
  mongo localhost:27017/plangeneratorflink -u pgf -p pwd --eval 'db.query_labels.remove({});db.query_grouping.remove({});db.query_observations.remove({});db.query_placement.remove({});'
}


restartLocalMongoDB(){
  echo "\n\n********* RESTART MONGODB *********\n\n"
  cd $directory/$plangeneratorflinkManagementDirName/$mongoDBFoldername
  docker-compose down && docker-compose up -d
  ping localhost -c 15 > /dev/null
}


start_local_flink_cluster(){
  echo "\n\n********* START FLINK CLUSTER *********\n\n"
  "$directory"/"$flinkDirName"/build-target/bin/start-cluster.sh
}





echo "========= build started ========="

# Initialize our intern variables
flinkDirName="flink-observation"
plangeneratorflinkDirName="zerotune-plan-generation"
plangeneratorflinkManagementDirName="zerotune-management"
mongoDBFoldername="mongoDBLocal"



OPTIND=1         # Reset in case getopts has been used previously in the shell.

# Initialize our customizable variables:
directory="$HOME/ZeroTune"
debugConfig=0
plangeneratorflink=0
flink=0
flinkFromScratch=0
run=0
endCluster=0
cleanLogDirAndMongoDB=0
restartMongoDB=0

while getopts "d:DpfFrcmeh" opt; do
  case "$opt" in
    h|\?)
      show_help
      exit 0
      ;;
    d)  directory=$OPTARG
      ;;
    D)  debugConfig=1
      ;;
    p)  plangeneratorflink=1
      ;;
    f)  flink=1
      ;;
    F)  flink=1; flinkFromScratch=1
      ;;
    r)  run=1
      ;;
    e)  endCluster=1
      ;;
    c)  cleanLogDirAndMongoDB=1
      ;;
    m)  restartMongoDB=1
      ;;
  esac
done

shift $((OPTIND-1))

[ "${1:-}" = "--" ] && shift



if [ "$run" = "1" ] || [ "$endCluster" = "1" ] ; then
  stop_local_flink_cluster
fi


if [ "$flink" = "1" ] ; then
  build_flink $flinkFromScratch
fi


if [ "$plangeneratorflink" = "1" ] ; then
  build_plangeneratorflink
  copy_plangeneratorflink_to_flink_lib
fi


if [ "$debugConfig" = "1" ] ; then
  copy_custom_config_to_flink_build
fi

if [ "$restartMongoDB" = "1" ] ; then
  restartLocalMongoDB
fi

if [ "$cleanLogDirAndMongoDB" = "1" ] ; then
  clean_log_dir_and_mongoDB
fi

if [ "$run" = "1" ] ; then
  start_local_flink_cluster
fi
 
echo "========= build finished ========="
