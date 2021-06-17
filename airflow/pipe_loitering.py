import posixpath as pp
import datetime as dt

from airflow import DAG
from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.models import Variable

from airflow_ext.gfw import config as config_tools
from airflow_ext.gfw.models import DagFactory
from airflow_ext.gfw.operators.bigquery_operator import BigQueryCreateEmptyTableOperator
from airflow_ext.gfw.operators.dataflow_operator import DataFlowDirectRunnerOperator


PIPELINE='pipe_loitering'


class PipeLoiteringDagFactory(DagFactory):

    def __init__(self, pipeline=PIPELINE, **kwargs):
        super(PipeLoiteringDagFactory, self).__init__(pipeline=pipeline, **kwargs)

    def build(self, dag_id):
        config = self.config

        start_date, end_date = self.source_date_range()

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:
            source_exists = BigQueryCheckOperator(
                task_id='source_exists',
                sql="""
                SELECT count(*) FROM `{project_id}.{source_messages_dataset}.{source_messages_table}` WHERE _PARTITIONTIME BETWEEN '{{{{ds}}}}' AND '{{{{next_ds}}}}'
                """.format(**config),
                use_legacy_sql=False,
                retries=144,
                retry_delay=dt.timedelta(minutes=30),
                max_retry_delay=dt.timedelta(minutes=30),
                on_failure_callback=config_tools.failure_callback_gfw
            )

            create_raw_loitering = DataFlowDirectRunnerOperator(
                task_id='create-raw-loitering',
                pool='dataflow',
                py_file=Variable.get('DATAFLOW_WRAPPER_STUB'),
                options=dict(
                    startup_log_file=pp.join(Variable.get('DATAFLOW_WRAPPER_LOG_PATH'),
                                             'pipe_loitering/create-raw-loitering.log'),
                    command='{docker_run} {docker_image} create_raw_loitering'.format(**config),
                    project=config['project_id'],
                    runner='{dataflow_runner}'.format(**config),
                    start_date=start_date,
                    end_date=end_date,
                    temp_location='gs://{temp_bucket}/dataflow_temp'.format(**config),
                    staging_location='gs://{temp_bucket}/dataflow_staging'.format(**config),
                    max_num_workers='{dataflow_max_num_workers}'.format(**config),
                    disk_size_gb='{dataflow_disk_size_gb}'.format(**config),
                    source='{project_id}.{source_messages_dataset}.{source_messages_table}'.format(**config),
                    sink='{project_id}:{pipeline_dataset}.{raw_table}'.format(**config),
                )
            )

            dag >> source_exists >> create_raw_loitering

            if not config.get('backfill', False):
                merge_raw_loitering = self.build_docker_task({
                    'task_id':'merge-raw-loitering',
                    'pool':'bigquery',
                    'docker_run':'{docker_run}'.format(**config),
                    'image':'{docker_image}'.format(**config),
                    'name':'loitering-merge-raw-loitering',
                    'dag':dag,
                    'arguments':[
                        'merge_raw_loitering',
                        '--source', '{project_id}.{pipeline_dataset}.{raw_table}'.format(**config),
                        '--destination', '{project_id}.{pipeline_dataset}.{loitering_table}'.format(**config),
                     ]
                })

                create_raw_loitering >> merge_raw_loitering

            return dag


for mode in ['daily', 'monthly', 'yearly']:
    dag_id='loitering_{}'.format(mode)
    globals()[dag_id] = PipeLoiteringDagFactory(schedule_interval='@{}'.format(mode)).build(dag_id)
