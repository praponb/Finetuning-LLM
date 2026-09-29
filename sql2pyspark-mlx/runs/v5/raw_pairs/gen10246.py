from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("employee_job_title").orderBy("is_remote_flag", "id")
w3 = Window.partitionBy("manager_id", "employee_job_title").orderBy(F.col("is_remote_flag_avg_6").desc_nulls_first(), "id")

base = (
    df_employees
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-02-06"))) & F.month("hire_date").isin([3, 7, 10]))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "level",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.coalesce("job_title", F.lit("NONE")).alias("job_title_filled"),
    )
)

enriched = (
    base
    .alias("e")
    .join(df_employees.alias("p2"), F.col("e.manager_id") == F.col("p2.id"), "inner")
    .filter(F.col("p2.level") == "L3")
    .select(
        "e.id",
        "e.email",
        "e.manager_id",
        "e.is_remote_flag",
        "e.job_title_filled",
        F.col("p2.salary"),
        F.col("p2.job_title").alias("employee_job_title"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.avg("is_remote_flag").over(w).alias("is_remote_flag_avg_6"),
        F.ntile(5).over(w2).alias("is_remote_flag_bucket"),
    )
)

ranked = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select(
        "id",
        "email",
        "manager_id",
        "is_remote_flag",
        "salary",
        "employee_job_title",
        "is_remote_flag_bucket",
        "rank_in_group",
    )
    .orderBy("id")
)
