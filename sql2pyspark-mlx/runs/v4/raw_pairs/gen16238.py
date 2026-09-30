from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy("delivered_date", "id")
w2 = Window.partitionBy("status").orderBy(F.col("shipped_date").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("channel").orderBy(F.col("discount_avg_5_prior").desc_nulls_last(), "id")

deduped = (
    df_shipments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_dims = (
    deduped
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select("s.*", F.col("o.channel"), F.col("o.discount"))
)

with_dims_shipments = (
    with_dims
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.status").alias("order_status"), F.col("p.amount"))
)

scored = (
    with_dims_shipments
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("discount_avg_5_prior", F.avg("discount").over(w3))
)

with_dims_shipments_2 = (
    scored
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select(
        "s.*",
        F.col("p.status").alias("order_status_2"),
        F.col("p.discount").alias("order_discount"),
        F.col("p.order_date"),
    )
)

enriched = (
    with_dims_shipments_2
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select("e.*", F.col("o.discount").alias("order_discount_2"))
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .select(
        "id",
        "order_id",
        F.col("order_discount").alias("ORDER_DISCOUNT_VAL"),
        F.col("discount").alias("DISCOUNT_VAL"),
        F.col("amount").alias("AMOUNT_VAL"),
    )
    .unpivot(
        ["id", "order_id"],
        ["ORDER_DISCOUNT_VAL", "DISCOUNT_VAL", "AMOUNT_VAL"],
        "metric",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
