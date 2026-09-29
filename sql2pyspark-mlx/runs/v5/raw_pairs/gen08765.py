from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("days_since_hire_date").desc_nulls_last())
w_part = Window.partitionBy("job_title").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("days_since_hire_date").asc_nulls_first())

filtered = (
    df_employees
    .filter(F.col("hire_date") >= F.to_date(F.lit("2023-04-27")))
    .withColumn("days_since_hire_date", F.datediff(F.current_date(), "hire_date"))
)

ranked = (
    filtered
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    ranked
    .filter(F.col("salary").between(104000, 125000))
    .select(
        "id",
        "hire_date",
        "level",
        "latest_rank",
        F.max("days_since_hire_date").over(w_part).alias("running_max_days_since_hire_date"),
        F.count("days_since_hire_date").over(w_part).alias("running_count_days_since_hire_date"),
        F.sum("days_since_hire_date").over(w_part).alias("running_sum_days_since_hire_date"),
        F.avg("days_since_hire_date").over(w_part).alias("running_avg_days_since_hire_date"),
        F.rank().over(w3).alias("days_since_hire_date_rank"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("latest_rank") >= 1)
    .withColumn("running_sum_days_since_hire_date_whole", F.ceil("running_sum_days_since_hire_date"))
)

summary = (
    cleaned
    .withColumn("year_start", F.date_trunc("YEAR", "hire_date"))
    .groupingSets([["level", "year_start"], ["level"], []], "level", "year_start")
    .agg(
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"),
        F.round(F.sum("running_avg_days_since_hire_date") / F.nullif(F.count(F.when(F.col("level") == "L3", 1)), F.lit(0)), 2).alias("running_avg_days_since_hire_date_per_l3"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("l2_count") > 1)
)

result = (
    summary
    .select("l2_count", "running_avg_days_since_hire_date_per_l3", "gid")
    .orderBy(F.col("l2_count").desc_nulls_last())
)
