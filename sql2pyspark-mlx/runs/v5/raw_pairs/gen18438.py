from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")
w2 = Window.partitionBy("city").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w5 = Window.partitionBy("customer_id", "attr_key").orderBy(F.col("credit_limit").desc_nulls_first())
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

enriched = (
    flattened
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select("src.*", F.col("c.email"))
)

with_dims = (
    enriched
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .select(
        "s.customer_id",
        "s.created_at",
        "s.coupon_codes",
        "s.attr_key",
        "s.value",
        "s.email",
        F.col("d.city"),
        F.col("d.credit_limit"),
    )
)

with_windows = (
    with_dims
    .withColumn("attr_key_avg_credit_limit", F.avg("credit_limit").over(w))
    .withColumn("credit_limit_count_30m", F.count("credit_limit").over(w2))
    .withColumn("credit_limit_count_30m_2", F.count("credit_limit").over(w3))
    .withColumn("credit_limit_count_15m", F.count("credit_limit").over(w4))
    .withColumn("credit_limit_percent_rank", F.percent_rank().over(w5))
)

valid = (
    with_windows
    .filter(F.col("city").isNotNull() & (~F.col("attr_key").eqNullSafe(F.lit("weight_kg"))))
    .withColumn("city_email", F.concat("city", F.lit(" "), "email"))
)

stats = (
    valid
    .groupBy("customer_id")
    .agg(
        F.max("created_at").alias("last_seen"),
        F.min("created_at").alias("first_seen"),
        F.round(F.avg("credit_limit_count_15m"), 2).alias("avg_credit_limit_count_15m"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("attr_key_avg_credit_limit"))).alias("tokyo_total_attr_key_avg_credit_limit"),
        F.count(F.when(F.col("attr_key") == "weight_kg", 1)).alias("weight_kg_rows"),
    )
)

result = (
    stats
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "first_seen",
        "tokyo_total_attr_key_avg_credit_limit",
        "weight_kg_rows",
        F.col("min_account_balance"),
    )
    .orderBy(F.col("min_account_balance").desc())
)
