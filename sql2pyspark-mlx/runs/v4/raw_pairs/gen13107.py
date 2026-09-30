from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary", "id")
w2 = Window.partitionBy("job_title").orderBy(F.col("hire_date").asc_nulls_last(), "id")
w3 = Window.partitionBy("dept_id").orderBy(F.col("hire_date_month_end").desc_nulls_first(), "id")
w4 = Window.partitionBy("dept_id").orderBy(F.col("hire_date_month_end").desc(), "id")

cleaned = (
    df_employees
    .filter(F.col("manager_id").isNotNull() & F.col("full_name").like("%For%"))
    .withColumn("hire_date_month_end", F.last_day("hire_date"))
    .withColumn("email_lower", F.lower("email"))
    .withColumn("resolved_level", F.coalesce("level", F.lit("DIRECT")))
)

latest = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

ranked = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

latest_employees = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

latest_employees_2 = (
    latest_employees
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    latest_employees_2
    .alias("a")
    .join(df_departments.alias("p"), F.col("a.dept_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.location"))
)

result = (
    enriched
    .groupBy("manager_id")
    .pivot("job_title", ["Designer", "Manager"])
    .agg(F.sum("salary").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
