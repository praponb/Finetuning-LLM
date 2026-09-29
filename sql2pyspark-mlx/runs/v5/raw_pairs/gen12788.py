from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("code"))

with_dims = (
    exploded
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.email"))
)

enriched = (
    with_dims
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select(
        "s.*",
        F.col("d.credit_limit"),
        F.col("d.country"),
        F.col("d.email").alias("customer_email"),
    )
)

valid = (
    enriched
    .filter(F.col("customer_id").isNotNull())
    .withColumn("customer_email_code_key", F.concat_ws("-", "customer_email", "code"))
    .withColumn("country_filled", F.coalesce("country", F.lit("unassigned")))
    .withColumn("code_filled", F.coalesce("code", F.lit("OTHER")))
)

joined = (
    valid
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select(
        "src.customer_id",
        "src.credit_limit",
        "src.customer_email_code_key",
        F.col("d.age"),
        F.col("d.credit_limit").alias("customer_credit_limit"),
        F.col("d.email").alias("customer_email_2"),
    )
)

result = (
    joined
    .alias("s")
    .join(df_customers.alias("dim"), F.col("s.customer_id") == F.col("dim.id"), "inner")
    .groupBy(F.col("dim.segment").alias("segment"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("s.credit_limit")).alias("min_credit_limit"),
        F.avg(F.col("s.customer_credit_limit")).alias("avg_customer_credit_limit"),
    )
    .orderBy(F.col("cnt").desc_nulls_first())
)
