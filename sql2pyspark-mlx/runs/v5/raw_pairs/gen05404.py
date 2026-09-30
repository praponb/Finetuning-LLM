from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("dept_name").orderBy("hire_date", "id").rowsBetween(-3, Window.currentRow)

cleaned = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-04-26").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.datediff(F.current_date(), "hire_date").alias("days_since_hire_date"),
    )
)

enriched = (
    cleaned
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.dept_name"))
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    deduped
    .filter(F.col("hire_date") <= "2024-07-16")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "hire_date",
        "is_remote",
        "is_remote_flag",
        "days_since_hire_date",
        "dept_name",
        "rn",
        F.sum("is_remote_flag").over(w2).alias("last4_sum_is_remote_flag"),
        F.max("is_remote_flag").over(w2).alias("last4_max_is_remote_flag"),
        F.min("is_remote_flag").over(w2).alias("last4_min_is_remote_flag"),
        F.count("is_remote_flag").over(w2).alias("last4_count_is_remote_flag"),
    )
)

result = (
    metrics
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .cube(
        F.col("d.dept_name").alias("department_dept_name"),
        F.col("e.job_title").alias("job_title"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("e.last4_sum_is_remote_flag")).alias("max_last4_sum_is_remote_flag"),
        F.max(F.col("e.last4_max_is_remote_flag")).alias("max_last4_max_is_remote_flag"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("job_title").desc_nulls_last())
)
