from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "customer_id").orderBy(F.col("amount").desc_nulls_first(), "id")
w2 = Window.partitionBy("store_id", "channel").orderBy("order_date_month_end", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())

ranked = (
    df_orders
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

valid = (
    ranked
    .filter(F.col("customer_id").isNotNull() & (F.col("channel").eqNullSafe(F.lit("mobile"))))
    .select(
        "*",
        F.last_day("order_date").alias("order_date_month_end"),
        F.col("discount").cast("int").alias("discount_int"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

latest = (
    valid
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

with_dims = (
    latest
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id").eqNullSafe(F.col("p.id")), "inner")
    .select("e.*", F.col("p.store_name"), F.col("p.city"))
)

with_last = (
    with_dims
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.method").alias("payment_method"))
)

result = (
    with_last
    .groupBy(F.date_trunc("YEAR", "order_date").alias("year_start"))
    .agg(
        F.count_if(F.col("city") == "London").alias("london_events"),
        F.corr("amount", "discount").alias("corr_amount_discount"),
        F.sum("shipping_fee").alias("total_shipping_fee"),
        F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_count"),
    )
    .orderBy(F.col("london_events").desc_nulls_first())
)
