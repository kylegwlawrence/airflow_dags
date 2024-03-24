from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

image_name = "kylelawrence/python-alpine3-19:test1_k8_pod"
dag_name = f"docker-python-alpine"

with DAG(
    dag_id = dag_name,
    description='testing a simple python script in a docker container with an argument passed in',
    tags=['test'],
    params = {'n_elements': Param(110, type="integer")},
    render_template_as_native_obj=True # renders the n_elements param as an integer when passed to the task
) as dag:

    run_docker = KubernetesPodOperator(
        name="run_docker",
        image=image_name,
        cmds=["python", "main.py", "--n_elements", "{{ params.my_int_param }}"],
        task_id="run_docker",
        in_cluster=True,
        do_xcom_push=True,
        get_logs=True
    )

    run_docker