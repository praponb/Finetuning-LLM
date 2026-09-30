from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "element")
w2 = Window.partitionBy("element", "customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("element")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

flattened = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

flattened_carts = (
    flattened
    .select(
        "customer_id",
        "created_at",
        "product_id",
        "qty",
        "price",
        F.explode("coupon_codes").alias("element"),
    )
)

enriched = (
    flattened_carts
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.credit_limit"), F.col("d.age"))
)

cleaned = (
    enriched
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 20:00:00"))) & F.col("element").isNotNull() & F.col("age").isNotNull())
    .withColumn("price_times_credit_limit", (F.col("price") * F.col("credit_limit")))
)

joined = (
    cleaned
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.credit_limit").alias("customer_credit_limit"),
        F.col("b.age").alias("customer_age"),
    )
)

with_windows = (
    joined
    .select(
        "*",
        F.round((F.col("customer_credit_limit") / F.sum("customer_credit_limit").over(w)), 4).alias("customer_credit_limit_share"),
        F.count("age").over(w2).alias("age_count_15m"),
        F.round((F.col("price") / F.sum("price").over(w3)), 4).alias("price_share"),
    )
)

result = (
    with_windows
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "created_at",
        "product_id",
        "qty",
        "element",
        "credit_limit",
        "price_times_credit_limit",
        "customer_age",
        "customer_credit_limit_share",
        F.col("avg_account_balance"),
    )
    .orderBy(F.col("credit_limit").asc_nulls_last(), "customer_credit_limit_share")
)
