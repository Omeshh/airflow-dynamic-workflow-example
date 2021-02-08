def get_last_dag_run(dag):
    last_dag_run = dag.get_last_dagrun()
    if last_dag_run is None:
        return "1900-01-01 00:00:00UTC"
    else:
        return last_dag_run.execution_date.strftime("%Y-%m-%d %H:%M:%S%Z")
