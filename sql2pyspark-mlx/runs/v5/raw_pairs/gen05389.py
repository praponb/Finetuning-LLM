from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("level_length", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("dept_id").orderBy(F.col("last4_max_level_length").desc_nulls_last(), "id")
w3 = Window.partitionBy("dept_id").orderBy(F.col("manager_count").desc())
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.min("salary").alias("min_employee_salary"))

cleaned = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-08-29").cast("date")) & F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        F.length("level").alias("level_length"),
        F.substring("level", 1, 2).alias("level_prefix"),
    )
)

metrics = (
    cleaned
    .withColumn("last4_max_level_length", F.max("level_length").over(w))
    .withColumn("last4_sum_level_length", F.sum("level_length").over(w))
    .withColumn("last4_avg_level_length", F.avg("level_length").over(w))
)

valid = (
    metrics
    .filter((F.col("hire_date") >= F.lit("2023-04-14").cast("date")) & F.col("manager_id").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("job_title")).alias("job_title_title"),
        F.date_format("hire_date", "yyyy-MM").alias("hire_date_label"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

joined = (
    latest
    .alias("s")
    .join(df_employees.alias("d"), F.col("s.manager_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.level"), F.col("d.email"), F.col("d.salary"))
)

daily = (
    joined
    .groupBy("dept_id", "manager_id")
    .agg(
        F.sum(F.when(F.col("level") == "L2", F.col("last4_max_level_length")).otherwise(0)).alias("l2_last4_max_level_length"),
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_count"),
        F.percentile_approx("last4_avg_level_length", 0.5).alias("last4_avg_level_length_p50"),
        F.round(F.avg("last4_sum_level_length"), 2).alias("avg_last4_sum_level_length"),
    )
    .withColumn("manager_count_dense_rank", F.dense_rank().over(w3))
)

result = (
    daily
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select("dept_id", "manager_id", "manager_count", F.col("min_employee_salary"))
    .orderBy(F.col("manager_count").desc_nulls_first())
)
