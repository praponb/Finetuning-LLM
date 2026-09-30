from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("code"))
)

base = (
    flattened
    .filter((F.col("created_at") >= F.lit("2025-03-02 01:00:00").cast("timestamp")) & F.col("customer_id").isNotNull())
    .withColumn("code_prefix", F.substring("code", 1, 4))
)

base_carts = (
    base
    .filter((F.col("created_at") > F.lit("2025-03-02 01:00:00").cast("timestamp")) & (~F.col("code").eqNullSafe(F.lit("BUNDLE"))))
    .select(
        "id",
        "customer_id",
        "created_at",
        "code_prefix",
        F.date_format("created_at", "yyyy-MM-dd").alias("created_at_label"),
        F.coalesce("code", F.lit("N/A")).alias("code_clean"),
    )
)

joined = (
    base_carts
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.created_at",
        "e.created_at_label",
        F.col("c.credit_limit"),
        F.col("c.email"),
    )
)

result = joined.select("id", "customer_id", "created_at_label", "credit_limit")
