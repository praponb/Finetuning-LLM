from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("id", "customer_id", F.explode_outer("attributes").alias("k", "value"))
)

valid = (
    exploded
    .filter(F.col("k").isNotNull() & F.col("customer_id").isin([10, 14]))
    .withColumn("k_prefix", F.substring("k", 1, 3))
)

joined = (
    valid
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.id", "a.customer_id", F.col("p.signup_date"), F.col("p.segment"))
)

enriched = (
    joined
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .select("e.customer_id", "e.signup_date", "e.segment", F.col("d.credit_limit"))
)

result = (
    enriched
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .groupBy(
        F.col("p.country").alias("country"),
        F.col("a.segment").alias("cart_segment"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("a.credit_limit")).alias("min_credit_limit"),
    )
)
