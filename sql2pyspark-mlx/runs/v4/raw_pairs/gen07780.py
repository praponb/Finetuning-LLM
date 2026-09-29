from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")
w2 = Window.partitionBy("manager_id", "level").orderBy("salary_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("level", "job_title").orderBy(F.col("salary_rounded").desc_nulls_last())

valid = (
    df_employees
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-10-13"))) & F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.round("salary", 0).alias("salary_rounded"),
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
)

result = (
    valid
    .select(
        "*",
        F.avg("salary_rounded").over(w).alias("manager_id_avg_salary_rounded"),
        F.first("is_remote_flag", ignorenulls=True).over(w2).alias("first_known_is_remote_flag"),
        F.dense_rank().over(w_grp).alias("salary_rounded_dense_rank"),
    )
)
