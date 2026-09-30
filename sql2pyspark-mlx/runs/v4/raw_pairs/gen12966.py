from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("salary", "id")
w2 = Window.partitionBy("dept_id", "job_title").orderBy("salary")
w3 = Window.partitionBy("manager_id").orderBy("salary", "id")
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.max("salary").alias("max_employee_salary"))

enriched = (
    df_employees
    .withColumn("next_salary", F.lead("salary", 2).over(w_running))
    .withColumn("row_seq", F.row_number().over(w_running))
    .withColumn("salary_dense_rank", F.dense_rank().over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)

result = (
    enriched
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "salary",
        "is_remote",
        "level",
        "next_salary",
        "row_seq",
        F.col("max_employee_salary"),
    )
    .orderBy("manager_id")
)
