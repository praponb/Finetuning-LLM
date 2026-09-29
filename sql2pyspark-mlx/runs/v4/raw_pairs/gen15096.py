from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("plan").orderBy("monthly_fee", "id")
w3 = Window.orderBy(F.col("monthly_fee").desc(), "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("plan_length").desc(), "id")

cleaned = (
    df_subscriptions
    .filter(F.col("start_date") >= F.to_date(F.lit("2023-08-25")))
    .select(
        "id",
        "customer_id",
        "plan",
        "monthly_fee",
        F.length("plan").alias("plan_length"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.count("plan_length").over(w).alias("customer_id_count_plan_length"),
        F.ntile(10).over(w2).alias("monthly_fee_bucket"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

latest = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

stats = (
    latest
    .groupBy("plan")
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.percentile_approx("monthly_fee", 0.95).alias("monthly_fee_p95"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
    )
)

result = stats.select("plan", "monthly_fee_p95")
