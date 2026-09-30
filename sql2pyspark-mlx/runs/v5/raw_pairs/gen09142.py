from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("coupon_codes").alias("element"),
    )
)

filtered = (
    lines
    .filter(F.col("element").isNotNull() & (~F.col("element").eqNullSafe(F.lit("WELCOME5"))))
    .withColumn("created_at_date", F.to_date("created_at"))
)

with_dims = (
    filtered
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.city") != "Tokyo")
    .select("e.*", F.col("p.signup_date"), F.col("p.name"), F.col("p.segment"))
)

enriched = (
    with_dims
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select(
        "e.customer_id",
        "e.created_at",
        "e.element",
        "e.signup_date",
        F.col("p.age"),
        F.col("p.name").alias("customer_name"),
        F.col("p.signup_date").alias("customer_signup_date"),
    )
)

joined = (
    enriched
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.city"))
)

result = (
    joined
    .groupBy(
        F.when(F.col("age") >= 65, "PREMIUM").otherwise("STANDARD").alias("age_tier"),
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
    )
    .agg(
        F.percentile_approx("age", 0.9).alias("age_p90"),
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.sum(F.when(F.col("element") == "VIP", F.col("age")).otherwise(0)).alias("vip_age"),
        F.percentile_approx("age", 0.95).alias("age_p95"),
    )
    .orderBy("hour_window")
)
