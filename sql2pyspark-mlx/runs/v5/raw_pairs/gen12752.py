from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc_nulls_last())
w3 = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("status").orderBy(F.col("discount").desc(), "id")
w6 = Window.partitionBy("status").orderBy("status_sum_discount", "id").rowsBetween(-3, Window.currentRow)
w_ordered = Window.orderBy("shipping_fee_percent_rank_minus_discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-02-26").cast("date")) & (F.col("store_id") == 27))
    .select(
        "id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        F.greatest("shipping_fee", "amount").alias("greatest_shipping_fee_amount"),
    )
)

enriched = (
    cleaned
    .filter(F.col("amount").between(590, 1490))
    .select(
        "*",
        F.sum("discount").over(w).alias("status_sum_discount"),
        F.percent_rank().over(w2).alias("shipping_fee_percent_rank"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") <= F.col("s.order_date")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.delivered_date").alias("shipment_delivered_date"))
)

cleaned_orders = (
    with_last
    .filter(F.col("status") == "COMPLETED")
    .select(
        "id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        "greatest_shipping_fee_amount",
        "status_sum_discount",
        "event_seq",
        (F.col("shipping_fee_percent_rank") - F.col("discount")).alias("shipping_fee_percent_rank_minus_discount"),
    )
)

enriched_orders = (
    cleaned_orders
    .withColumn("event_seq_2", F.row_number().over(w5))
    .withColumn("greatest_shipping_fee_amount_avg_3", F.avg("greatest_shipping_fee_amount").over(w6))
    .withColumn("last_known_amount", F.last("amount", ignorenulls=True).over(w_ordered))
    .withColumn("cumulative_amount", F.sum("amount").over(w8))
)

result = (
    enriched_orders
    .select(
        "id",
        "order_date",
        "status",
        "amount",
        "shipping_fee",
        "greatest_shipping_fee_amount",
        "status_sum_discount",
        "event_seq",
        "shipping_fee_percent_rank_minus_discount",
    )
)
