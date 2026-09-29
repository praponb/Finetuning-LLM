from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("tag"))

joined = (
    exploded
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.id", "a.customer_id", F.col("p.credit_limit"), F.col("p.city"))
)

valid = (
    joined
    .filter(F.col("city").isin(["Chiang Mai", "Singapore"]))
    .withColumn("credit_limit_filled", F.coalesce("credit_limit", F.lit(0)))
    .withColumn("city_filled", F.coalesce("city", F.lit("N/A")))
)

filtered = (
    valid
    .filter(F.col("city").isNotNull())
    .withColumn("credit_limit_int", F.col("credit_limit").cast("int"))
    .withColumn("city_clean", F.coalesce("city", F.lit("NONE")))
)

grouped = (
    filtered
    .groupBy("customer_id", "city")
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("city") == "Singapore", F.col("credit_limit_int"))).alias("singapore_total_credit_limit_int"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("city") == "Tokyo", 1)), F.lit(0)), 2).alias("credit_limit_per_tokyo"),
        F.percentile_approx("credit_limit", 0.95).alias("credit_limit_p95"),
        F.count_if(F.col("city") == "Tokyo").alias("tokyo_events"),
    )
)

result = (
    grouped
    .select(
        "customer_id",
        "city",
        "num_rows",
        "credit_limit_per_tokyo",
        "credit_limit_p95",
        "tokyo_events",
    )
    .orderBy("customer_id")
)
