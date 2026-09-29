from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("employee_level").orderBy(F.col("budget").desc_nulls_last())
w4 = Window.partitionBy("employee_job_title")
w5 = Window.partitionBy("location").orderBy(F.col("budget").desc())
w6 = Window.partitionBy("employee_job_title", "employee_level").orderBy("row_num", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.partitionBy("employee_job_title").orderBy(F.col("row_num").desc(), "id")
w8 = Window.partitionBy("location").orderBy(F.col("budget_dense_rank").desc_nulls_last(), "id")

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & F.col("location").isin(["Tokyo", "Chiang Mai"]))
    .select(
        "*",
        F.lower("dept_name").alias("dept_name_lower"),
        F.coalesce("dept_name", F.lit("UNKNOWN")).alias("dept_name_clean"),
    )
)

valid = (
    cleaned
    .filter(~F.col("location").isin(["Tokyo", "London", "Bangkok"]))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "dept_name_lower",
        F.substring("location", 1, 4).alias("location_prefix"),
        F.lower("location").alias("location_lower"),
        F.coalesce("dept_name", F.lit("DIRECT")).alias("dept_name_clean_2"),
    )
)

ranked = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

eligible = (
    ranked
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.is_remote")), "left_anti")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location",
        "s.budget",
        "s.location_prefix",
        "s.location_lower",
        "s.dept_name_clean_2",
        "s.row_num",
        F.col("c.job_title").alias("employee_job_title"),
        F.col("c.level").alias("employee_level"),
    )
)

with_windows = (
    with_latest
    .filter(F.col("location") == "London")
    .select(
        "id",
        "location",
        "dept_name_clean_2",
        "employee_job_title",
        F.percent_rank().over(w3).alias("budget_percent_rank"),
        F.round((F.col("budget") / F.sum("budget").over(w4)), 4).alias("budget_share"),
        F.dense_rank().over(w5).alias("budget_dense_rank"),
        F.avg("budget").over(w6).alias("budget_avg_5"),
        F.row_number().over(w7).alias("row_seq"),
    )
)

deduped = (
    with_windows
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .groupBy("employee_job_title")
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.when(F.col("location") == "London", F.col("budget_avg_5"))).alias("london_max_budget_avg_5"),
    )
    .filter(F.col("cnt") < 1000)
)
