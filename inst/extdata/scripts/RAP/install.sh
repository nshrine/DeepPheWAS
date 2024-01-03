#!/bin/bash

if [ $# -lt 2 ]; then
    echo "Usage: $0 <projectid> <project directory>"
    exit 1
fi

PROJECT_ID=$1
PROJECT_DIR=$2
DOCKER_SAVE=DeepPheWAS.docker.tar.gz
EXTRA_OPTIONS=/tmp/extraOptions.json
DXCOMPILER=/tmp/dxCompiler.jar
WDL="phenotype_generation.wdl association_testing.wdl"

dx select $PROJECT_ID && \
    dx mkdir -p $PROJECT_DIR &&
    dx cd $PROJECT_DIR && \
    dx upload `dirname $0`/../docker/Dockerfile && \
    dx run --brief -y --wait swiss-army-knife \
        -iin=Dockerfile \
        -icmd="docker build -t nshrine/deep_phewas . && docker save nshrine/deep_phewas | gzip > $DOCKER_SAVE" && \
    DOCKER_FILE_ID=`dx describe $DOCKER_SAVE | awk '$1 == "ID" { print $2 }'` &&
    cat <<-JSON > $EXTRA_OPTIONS &&
	{
	    "defaultRuntimeAttributes" : {
	    "docker" : "dx://${DOCKER_FILE_ID}"
	    }
	}	
JSON
    wget https://github.com/dnanexus/dxCompiler/releases/download/2.11.4/dxCompiler-2.11.4.jar -O $DXCOMPILER && \
    cd `dirname $0`/../../WDL && \
    for i in $WDL; do
        java -jar $DXCOMPILER compile $i -extras $EXTRA_OPTIONS -project $PROJECT_ID -folder $PROJECT_DIR -streamFiles perfile -f
    done
