from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

image_name = "kylelawrence/python-alpine3-19:test1_k8_pod "
dag_name = f"docker-{image_name.split('/')[1].replace(':','_')}"

with DAG(
    dag_id = dag_name,
    description='testing a simple python script in a docker container with an argument passed in',
    tags=['test']
) as dag:

    run_docker = KubernetesPodOperator(
        name="run_docker",
        image=image_name,
        cmds=["python", "main.py", "--n_elements", "116"],
        task_id="run_docker",
        in_cluster=True,
        do_xcom_push=True,
        get_logs=True
    )

    run_docker