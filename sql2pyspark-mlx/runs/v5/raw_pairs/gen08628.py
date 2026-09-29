from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("channel").orderBy("amount", "id")
w2 = Window.partitionBy("store_id", "channel")

enriched = (
    df_orders
    .select(
        "id",
        "order_date",
        "discount",
        F.lead("amount").over(w_recent).alias("next_amount"),
        F.lag("amount").over(w_recent).alias("prev_amount"),
        F.lag("shipping_fee").over(w_recent).alias("prev_shipping_fee"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.avg("shipping_fee").over(w2).alias("store_id_avg_shipping_fee"),
    )
)

result = (
    enriched
    .groupBy(
        "seq_no",
        F.when(F.col("prev_amount") >= 1420, "LARGE").otherwise("SMALL").alias("prev_amount_tier"),
        F.when(F.col("discount") >= 0.44, "HIGH").otherwise("LOW").alias("discount_tier"),
    )
    .agg(
        F.avg("prev_shipping_fee").alias("avg_prev_shipping_fee"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("avg_prev_shipping_fee") < 1000000)
    .orderBy(F.col("avg_discount").desc_nulls_first())
)
