from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("order_id").orderBy("cost", "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("discount").desc(), "id")

enriched = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .select(
        "e.id",
        "e.order_id",
        "e.carrier",
        "e.delivered_date",
        "e.cost",
        F.col("d.shipping_fee"),
        F.col("d.discount"),
        F.col("d.amount"),
    )
)

enriched_shipments = (
    enriched
    .filter(F.col("shipping_fee") >= 40)
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "discount",
        F.lag("cost").over(w_part).alias("prev_cost"),
        F.lead("cost").over(w_part).alias("next_cost"),
        F.lead("amount").over(w_part).alias("next_amount"),
        F.lag("amount").over(w_part).alias("prev_amount"),
        F.row_number().over(w_part).alias("row_seq"),
    )
)

with_dims = (
    enriched_shipments
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.status"), F.col("p.shipping_fee"))
)

deduped = (
    with_dims
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

enriched_shipments_2 = (
    deduped
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "inner")
    .select(
        "a.*",
        F.col("p.channel"),
        F.col("p.discount").alias("order_discount"),
        F.col("p.amount"),
    )
)

result = (
    enriched_shipments_2
    .groupBy(
        F.when(F.col("next_amount") >= 1340, "HIGH").otherwise("LOW").alias("next_amount_tier"),
    )
    .agg(
        F.count_if(F.col("status") == "CANCELLED").alias("cancelled_events"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.size(F.collect_set("status")).alias("num_status_2"),
        F.min("next_amount").alias("min_next_amount"),
        F.percentile_approx("discount", 0.9).alias("discount_p90"),
    )
    .filter(F.col("num_status") > 1)
    .orderBy(F.col("num_status").desc_nulls_first())
)
