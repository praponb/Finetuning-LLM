from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "level").orderBy("salary", "id")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.min("salary").alias("min_employee_salary"))

deduped = (
    df_employees
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    deduped
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.when(F.col("level").rlike("^(L5|L1)$"), "EXTENDED").when(F.col("level").rlike("^(L2|L3)$"), "CORE").otherwise("PRIORITY").alias("level_zone"),
        F.col("min_employee_salary"),
    )
    .orderBy(F.col("level_zone").asc_nulls_first())
)
