from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("status", "customer_id").orderBy(F.col("sqft").desc_nulls_last())
w3 = Window.orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("discount", "id")
w5 = Window.partitionBy("status", "store_id").orderBy("sqft", "id")

cleaned = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-07-19").cast("date")) & (F.col("order_date") >= "2025-08-01"))
    .select(
        "*",
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
        (F.col("shipping_fee") + F.col("discount")).alias("shipping_fee_plus_discount"),
    )
)

joined = (
    cleaned
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id").eqNullSafe(F.col("s.id")), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.store_id",
        "src.order_date",
        "src.status",
        "src.channel",
        "src.amount",
        "src.discount",
        "src.shipping_fee",
        "src.channel_filled",
        "src.shipping_fee_plus_discount",
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
    )
)

with_latest = (
    joined
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.order_date",
        "s.status",
        "s.discount",
        "s.channel_filled",
        "s.sqft",
        "s.store_name",
        "s.opened_date",
        F.col("c.amount").alias("payment_amount"),
    )
)

with_windows = (
    with_latest
    .select(
        "id",
        "status",
        "discount",
        "payment_amount",
        F.rank().over(w2).alias("sqft_rank"),
        F.first("payment_amount", ignorenulls=True).over(w3).alias("first_known_payment_amount"),
        F.ntile(5).over(w4).alias("discount_bucket"),
        F.lead("sqft", 1).over(w5).alias("next_sqft"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("first_known_payment_amount") >= 1330, "LARGE").otherwise("SMALL").alias("first_known_payment_amount_tier"),
    )
    .agg(
        F.max("next_sqft").alias("max_next_sqft"),
        F.round(F.avg("sqft_rank"), 2).alias("avg_sqft_rank"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.avg("first_known_payment_amount"), 2).alias("avg_first_known_payment_amount"),
    )
    .filter(F.col("avg_sqft_rank") < 1000000)
    .orderBy(F.col("avg_first_known_payment_amount").desc_nulls_last())
)
