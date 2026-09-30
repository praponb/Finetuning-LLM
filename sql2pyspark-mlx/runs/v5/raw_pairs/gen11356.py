from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("coupon_codes").alias("element"),
    )
)

with_dims = (
    flattened
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .filter(F.col("d.name").like("%Diaz%"))
    .select("s.id", "s.customer_id", F.col("d.credit_limit"))
)

valid = (
    with_dims
    .filter(F.col("credit_limit") >= 14800)
    .select(
        "*",
        F.round((F.col("credit_limit") * 0.9), 2).alias("credit_limit_adjusted"),
    )
)

enriched = (
    valid
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select(
        "src.*",
        F.col("c.segment"),
        F.col("c.credit_limit").alias("customer_credit_limit"),
        F.col("c.signup_date"),
    )
)

result = (
    enriched
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("segment") == "Consumer", F.col("customer_credit_limit"))).alias("consumer_total_customer_credit_limit"),
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
        F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_count"),
        F.sum("customer_credit_limit").alias("total_customer_credit_limit"),
    )
    .filter(F.col("smb_count") > 1)
    .orderBy(F.col("customer_id").desc())
)
