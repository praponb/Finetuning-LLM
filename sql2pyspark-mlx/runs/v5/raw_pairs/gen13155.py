from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("code"))

cleaned = (
    lines
    .filter(F.col("code") == "BUNDLE")
    .withColumn("code_lower", F.lower("code"))
)

joined = (
    cleaned
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.id", "e.customer_id", F.col("p.signup_date"), F.col("p.email"))
)

with_dims = (
    joined
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.signup_date").alias("customer_signup_date"))
)

result = (
    with_dims
    .groupBy(
        "customer_id",
        F.date_trunc("MONTH", "customer_signup_date").alias("activity_month"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("activity_month").desc_nulls_first())
)
