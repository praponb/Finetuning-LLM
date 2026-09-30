from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

base = (
    lines
    .filter(F.col("customer_id").isNotNull() & (~F.col("value").eqNullSafe(F.lit("WELCOME5"))))
    .withColumn("created_at_label", F.date_format("created_at", "yyyy-MM"))
    .withColumn("resolved_value", F.coalesce("value", F.lit("N/A")))
)

valid = (
    base
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 15:00:00"))) & F.col("customer_id").isNull())
    .withColumn("is_vip", F.when(F.col("value") == "VIP", 1).otherwise(0))
    .withColumn("created_at_day", F.col("created_at").cast("date"))
)

with_dims = (
    valid
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select(
        "src.customer_id",
        "src.created_at",
        "src.created_at_label",
        "src.created_at_day",
        F.col("c.credit_limit"),
    )
)

cleaned = (
    with_dims
    .filter((F.to_date("created_at") >= "2025-04-19") & (F.col("created_at") > F.lit("2025-03-01").cast("date")))
    .select(
        "customer_id",
        "created_at",
        "created_at_day",
        "credit_limit",
        F.col("credit_limit").cast("int").alias("credit_limit_int"),
    )
)

with_dims_carts = (
    cleaned
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.age"))
)

result = (
    with_dims_carts
    .groupBy(F.date_trunc("WEEK", "created_at_day").alias("week_start"))
    .agg(
        F.max("created_at").alias("last_created_at"),
        F.min("created_at").alias("started_at"),
    )
    .orderBy(F.col("week_start").asc_nulls_last())
)
