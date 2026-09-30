from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "manager_id").orderBy(F.col("salary").desc(), "id")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.sum("salary").alias("sum_employee_salary"))

deduped = (
    df_employees
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.when(F.col("level").rlike("^(L4|L1)$"), "CORE").when(F.col("level").rlike("^(L2|L3)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("level_zone"),
        F.col("sum_employee_salary"),
    )
    .orderBy(F.col("id").desc_nulls_last(), "job_title")
)
