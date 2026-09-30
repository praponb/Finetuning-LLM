from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("monthly_fee_tier").orderBy(F.col("num_plans").desc())

base = (
    df_subscriptions
    .filter(F.col("end_date") < "2024-05-27")
    .withColumn("end_date_plus_41d", F.date_add("end_date", 41))
)

result = (
    base
    .groupBy(
        F.when(F.col("monthly_fee") >= 260, "PREMIUM").otherwise("STANDARD").alias("monthly_fee_tier"),
        F.date_trunc("week", "start_date").alias("week_start"),
    )
    .agg(
        F.percentile_approx("monthly_fee", 0.95).alias("monthly_fee_p95"),
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("plan")).alias("num_plans"),
    )
    .filter(F.col("num_rows") > 1)
    .withColumn("num_plans_percent_rank", F.percent_rank().over(w))
)
