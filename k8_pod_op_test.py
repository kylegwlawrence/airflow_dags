from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

with DAG(
    "hello-dry-run",
    description='Runs bash command pip list',
    tags=['example']
) as dag:


    k = KubernetesPodOperator(
        name="hello-dry-run",
        image="debian",
        cmds=["bash", "-cx"],
        arguments=["echo", "10"],
        labels={"foo": "bar"},
        task_id="dry_run_demo",
        tags = ['test'],
        do_xcom_push=True
    )

    k.dry_run()