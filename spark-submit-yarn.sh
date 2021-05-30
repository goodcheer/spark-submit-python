#!/bin/bash

export HADOOP_USER_NAME=myname

# environment is an alias for conda archive. assign python interpreter under the env.
PYSPARK_PYTHON="./environment/bin/python"
CONDA_ARCHIVE=""
SDIST="dist/my-python-spark-app-0.0.1.zip"

SPARK_CMD="spark-submit \
--master local \
--deploy-mode client \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=${PYSPARK_PYTHON} \
--archive ${CONDA_ARCHIVE}#environment \
--py-files ${SDIST} \
./main.py -dt '$1'"

eval "${SPARK_CMD}"
