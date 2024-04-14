from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id = "ingest_all_stocks_data"
    , description='ingests data from Polygon API and other sources into postgres'
    , tags=['stocks','postgres']
    , params = {
        'user':'postgres'
        ,'password':'N5eG0AXAHw'
        , 'host':'raspberrypi3.local'
        , 'port':'35432'
        , 'database':'postgres'
        , 'schema':'stg'
        , 'windows':[9, 21, 50, 120]
        , 'tickers': ['FSLY', 'SNOW']
        }
    , default_args = {
        'image':'kylelawrence/python-stocks:latest'
        , 'in_cluster':True
        , 'do_xcom_push':True
        , 'get_logs':True
        , 'image_pull_policy':'cache'
        }
    , render_template_as_native_obj=True # renders a python non-string object as its python type when passed to the task, otherwise it gest converted to a string ?
    ) as dag:

        start = EmptyOperator(
            task_id="start"
            )
        dim_date = KubernetesPodOperator(
            name="dim_date",
            cmds=["python", f"dim_date.py", "--user", "{{ params.user }}", "--password", "{{ params.password }}", "--host", "{{ params.host }}", "--port", "{{ params.port }}", "--database", "{{ params.database }}", "--schema", "{{ params.schema }}"],
            task_id="dim_date"
        )
        dim_exchange = KubernetesPodOperator(
            name="dim_exchange",
            cmds=["python", f"dim_exchange.py", "--user", "{{ params.user }}", "--password", "{{ params.password }}", "--host", "{{ params.host }}", "--port", "{{ params.port }}", "--database", "{{ params.database }}", "--schema", "{{ params.schema }}"],
            task_id="dim_exchange"
        )
        dim_sp500 = KubernetesPodOperator(
            name="dim_sp500",
            #image=image_name,
            cmds=["python", f"dim_sp500.py", "--user", "{{ params.user }}", "--password", "{{ params.password }}"],
            task_id="dim_sp500"
        )
        dim_stock = KubernetesPodOperator(
            name="dim_stock",
            #image=image_name,
            cmds=["python", f"dim_stock.py", "--user", "{{ params.user }}", "--password", "{{ params.password }}"],
            task_id="dim_stock"
        )
        dim_ticker_type = KubernetesPodOperator(
            name="dim_ticker_type",
            #image=image_name,
            cmds=["python", f"dim_ticker_type.py", "--user", "{{ params.user }}", "--password", "{{ params.password }}"],
            task_id="dim_ticker_type"
        )
        fct_ema = KubernetesPodOperator(
            name="fct_ema",
            #image=image_name,
            cmds=["python", f"fct_ema.py", "--user", "{{ params.user }}", "--password", "{{ params.password }}", "--tickers", "{{ params.tickers }}", "--windows", "{{ params.windows }}"],
            task_id="fct_ema"
        )
        fct_macd = KubernetesPodOperator(
            name="fct_macd",
            #image=image_name,
            cmds=["python", f"fct_macd.py", "--user", "{{ params.user }}", "--password", "{{ params.password }}", "--tickers", "{{ params.tickers }}"],
            task_id="fct_macd"
        )
        fct_rsi = KubernetesPodOperator(
            name="fct_rsi",
            #image=image_name,
            cmds=["python", f"fct_rsi.py", "--user", "{{ params.user }}", "--password", "{{ params.password }}", "--tickers", "{{ params.tickers }}"],
            task_id="fct_rsi"
        )
        fct_stock_fincancials = KubernetesPodOperator(
            name="fct_stock_fincancials",
            #image=image_name,
            cmds=["python", f"fct_stock_fincancials.py", "--user", "{{ params.user }}", "--password", "{{ params.password }}", "--tickers", "{{ params.tickers }}"],
            task_id="fct_stock_fincancials"
        )
        fct_stocks = KubernetesPodOperator(
            name="fct_stocks",
            #image=image_name,
            cmds=["python", f"fct_stocks.py", "--user", "{{ params.user }}", "--password", "{{ params.password }}", "--days", "{{ params.days }}"],
            task_id="fct_stocks"
        )
        end = EmptyOperator(
            task_id="end"
            )
        start >> [dim_date, dim_sp500] >> end 
        start >> dim_ticker_type >> dim_exchange >> dim_stock >> fct_ema >> fct_macd >> fct_rsi >> fct_stock_fincancials >> fct_stocks >> end