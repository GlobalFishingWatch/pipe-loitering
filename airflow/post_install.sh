#!/usr/bin/env bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

python $AIRFLOW_HOME/utils/set_default_variables.py \
    --force docker_image=$1 \
    pipe_loitering \
    backfill="" \
    dag_install_path="${THIS_SCRIPT_DIR}" \
    dataflow_disk_size_gb="50" \
    dataflow_max_num_workers="200" \
    dataflow_runner="DataflowRunner" \
    docker_run="{{ var.value.DOCKER_RUN }}" \
    pipeline_bucket="{{ var.value.PIPELINE_BUCKET }}" \
    pipeline_dataset="{{ var.value.PIPELINE_DATASET }}" \
    project_id="{{ var.value.PROJECT_ID }}" \
    source_messages_dataset="gfw_research" \
    raw_table="raw_loitering_" \
    loitering_table="loitering" \

echo "Installation Complete"

