from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel", "customer_id")
w3 = Window.partitionBy("channel", "customer_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy("last_known_shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc(), "id")
w6 = Window.orderBy("last_known_shipping_fee", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("customer_id").orderBy("last_known_shipping_fee_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

filtered = (
    df_orders
    .filter(F.col("order_date") >= F.to_date(F.lit("2023-12-12")))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "shipping_fee",
        F.concat_ws("/", "status", "channel").alias("status_channel_key"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

enriched = (
    filtered
    .withColumn("last_known_shipping_fee", F.last("shipping_fee", ignorenulls=True).over(w))
    .withColumn("channel_max_shipping_fee", F.max("shipping_fee").over(w2))
    .withColumn("last_known_shipping_fee_2", F.last("shipping_fee", ignorenulls=True).over(w3))
)

enriched_orders = (
    enriched
    .select(
        "*",
        F.sum("last_known_shipping_fee").over(w4).alias("cumulative_last_known_shipping_fee"),
        F.ntile(5).over(w5).alias("shipping_fee_bucket"),
        F.last("shipping_fee", ignorenulls=True).over(w4).alias("last_known_shipping_fee_3"),
        F.avg("last_known_shipping_fee_2").over(w6).alias("last_known_shipping_fee_2_avg_3"),
        F.first("channel_max_shipping_fee", ignorenulls=True).over(w7).alias("first_known_channel_max_shipping_fee"),
    )
)

matched = (
    enriched_orders
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.order_date") >= F.col("pr.starts_on")) & (F.col("src.order_date") <= F.date_add(F.col("pr.ends_on"), 3)), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

attributed = (
    matched
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("a.order_date") >= F.col("pr.starts_on")) & (F.col("a.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

pivoted = (
    attributed
    .groupBy("store_id", "customer_id")
    .pivot("promotion_promo_code", ["VIP", "BUNDLE"])
    .agg(F.sum("shipping_fee_bucket").alias("total"))
)

result = (
    pivoted
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select("store_id", "customer_id", "VIP", F.col("sum_account_balance"))
    .orderBy(F.col("store_id").desc_nulls_last(), "sum_account_balance")
)
