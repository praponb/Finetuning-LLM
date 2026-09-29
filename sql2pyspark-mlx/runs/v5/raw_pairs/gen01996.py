from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())
w_grp = Window.partitionBy("customer_id").orderBy("amount", "id")
w3 = Window.partitionBy("status").orderBy("shipping_fee", "id")
w4 = Window.partitionBy("store_id").orderBy("next_shipping_fee", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("payment_status").orderBy(F.col("shipping_fee_bucket").desc(), "id")
w_running = Window.partitionBy("channel").orderBy("amount", "id")

with_last = (
    df_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") <= F.col("s.order_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("payment_status"))
)

enriched = (
    with_last
    .filter(F.month("order_date").isin([4, 9, 12]))
    .withColumn("next_discount", F.lead("discount").over(w_grp))
    .withColumn("next_shipping_fee", F.lead("shipping_fee").over(w_grp))
    .withColumn("row_seq", F.row_number().over(w_grp))
    .withColumn("shipping_fee_bucket", F.ntile(5).over(w3))
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

latest_match = (
    deduped
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") <= F.col("s.order_date")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("shipment_status"))
)

enriched_orders = (
    latest_match
    .select(
        "id",
        "channel",
        "discount",
        "next_discount",
        "next_shipping_fee",
        "row_seq",
        "shipping_fee_bucket",
        "dedup_rank",
        F.sum("row_seq").over(w6).alias("cumulative_row_seq"),
        F.ntile(4).over(w7).alias("shipping_fee_bucket_bucket"),
        F.ntile(4).over(w_running).alias("amount_bucket"),
    )
)

result = (
    enriched_orders
    .cube("channel")
    .agg(
        F.count_if(F.col("channel") == "partner").alias("partner_events"),
        F.round(F.sum("amount_bucket") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("amount_bucket_per_partner"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
        F.sum("next_discount").alias("total_next_discount"),
        F.sum("shipping_fee_bucket_bucket").alias("total_shipping_fee_bucket_bucket"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("amount_bucket_per_partner") > 0)
)
