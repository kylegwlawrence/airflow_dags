from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
import uuid

with DAG(
    "docker_test",
    description='testing a simple python script in a docker container with an argument passed in',
    tags=['test']
) as dag:

    run_docker = KubernetesPodOperator(
        name="run_docker",
        image="kylelawrence/k8_pod_airflow:test1",
        cmds=["--n_elements", "116"],
        labels={"uuid": uuid.uuid4().hex},
        task_id="run_docker",
        in_cluster=True,
        do_xcom_push=True,
        get_logs=True
    )

    run_docker