from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        F.explode_outer("attributes").alias("attr_key", "attr_value"),
    )
)

valid = (
    lines
    .filter(F.col("customer_id").isNotNull())
    .withColumn("attr_key_prefix", F.substring("attr_key", 1, 2))
)

joined = (
    valid
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.city"), F.col("d.credit_limit"))
)

joined_carts = (
    joined
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.country"))
)

cleaned = (
    joined_carts
    .filter(F.col("country").isNotNull())
    .withColumn("attr_key_group", F.when(F.col("attr_key").isin(["device", "weight_kg"]), "Group A").otherwise("Other"))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("credit_limit") >= 13300, "PREMIUM").otherwise("STANDARD").alias("credit_limit_tier"),
        "city",
    )
    .agg(
        F.percentile_approx("credit_limit", 0.9).alias("credit_limit_p90"),
        F.sum("credit_limit").alias("total_credit_limit"),
        F.percentile_approx("credit_limit", 0.9).alias("credit_limit_p90_2"),
    )
    .filter(F.col("credit_limit_p90") >= 0)
    .orderBy(F.col("credit_limit_tier").desc())
)
