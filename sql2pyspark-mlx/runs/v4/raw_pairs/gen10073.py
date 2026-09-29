from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("age").desc_nulls_first())
w_grp = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").asc_nulls_last())
w3 = Window.partitionBy("activity_month").orderBy(F.col("avg_age").desc())

valid = (
    df_subscriptions
    .filter(F.col("start_date") > F.lit("2023-08-24").cast("date"))
    .withColumn("status_plan", F.concat("status", F.lit(", "), "plan"))
)

joined = (
    valid
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.age"))
)

matched_keys = (
    joined
    .select("customer_id")
    .subtract(
        df_subscriptions
        .filter(F.col("plan").isin(["standard", "enterprise", "basic"]))
        .select("customer_id")
    )
)

verified = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter((F.col("end_date") > F.lit("2024-04-10").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("status_title", F.initcap(F.trim("status")))
)

enriched = (
    cleaned
    .withColumn("age_rank", F.rank().over(w))
    .withColumn("monthly_fee_dense_rank", F.dense_rank().over(w_grp))
)

result = (
    enriched
    .groupBy(
        F.date_trunc("MONTH", "end_date").alias("activity_month"),
        F.date_trunc("WEEK", "start_date").alias("week_start"),
    )
    .agg(
        F.avg("monthly_fee_dense_rank").alias("avg_monthly_fee_dense_rank"),
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("age_rank", [0.25, 0.5, 0.75]).alias("age_rank_quartiles"),
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("monthly_fee")).otherwise(0)).alias("expired_monthly_fee"),
        F.round(F.sum("monthly_fee_dense_rank") / F.nullif(F.count(F.when(F.col("status") == "EXPIRED", 1)), F.lit(0)), 2).alias("monthly_fee_dense_rank_per_expired"),
    )
    .withColumn("avg_age_dense_rank", F.dense_rank().over(w3))
    .orderBy(F.col("monthly_fee_dense_rank_per_expired").asc_nulls_last())
)
