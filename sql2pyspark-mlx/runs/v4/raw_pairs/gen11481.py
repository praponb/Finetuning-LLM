from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "channel").orderBy(F.col("shipping_fee").desc_nulls_last(), "id", "promo_id")
w2 = Window.partitionBy("status", "channel").orderBy(F.col("days_since_order_date").desc_nulls_first())
w3 = Window.partitionBy("status", "channel").orderBy(F.col("promo_id").asc_nulls_first())
w4 = Window.partitionBy("promo_code").orderBy(F.col("days_since_order_date").desc_nulls_first())
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

base = (
    df_orders
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
        (F.col("discount") - F.col("shipping_fee")).alias("discount_minus_shipping_fee"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

matched = (
    base
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.order_date") >= F.col("pr.starts_on")) & (F.col("e.order_date") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

deduped = (
    matched
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

scored = (
    deduped
    .withColumn("days_since_order_date_cume_dist", F.cume_dist().over(w2))
    .withColumn("promo_id_cume_dist", F.cume_dist().over(w3))
    .withColumn("days_since_order_date_dense_rank", F.dense_rank().over(w4))
)

result = (
    scored
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        "days_since_order_date",
        "discount_minus_shipping_fee",
        F.col("sum_account_balance"),
    )
    .orderBy("sum_account_balance")
)
