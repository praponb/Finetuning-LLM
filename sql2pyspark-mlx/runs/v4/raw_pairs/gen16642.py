from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")
w_recent = Window.orderBy("salary")
w_part = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_employees
    .select(
        "*",
        F.ntile(5).over(w_grp).alias("salary_bucket"),
        F.dense_rank().over(w_recent).alias("salary_dense_rank"),
        F.last("job_title", ignorenulls=True).over(w_part).alias("last_known_job_title"),
    )
)

result = (
    enriched
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "is_remote",
        "level",
        "salary_dense_rank",
        "last_known_job_title",
    )
    .orderBy(F.col("manager_id").desc())
)
