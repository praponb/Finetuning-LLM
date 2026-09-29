from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc_nulls_last())
w2 = Window.orderBy(F.col("days_delivered_date_to_shipped_date").desc(), "id")
w3 = Window.partitionBy("status").orderBy("shipped_date", "id")

valid = (
    df_shipments
    .filter((F.col("delivered_date") >= F.to_date(F.lit("2023-03-04"))) & F.col("order_id").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "delivered_date").alias("days_since_delivered_date"),
        F.datediff("shipped_date", "delivered_date").alias("days_delivered_date_to_shipped_date"),
    )
)

enriched = (
    valid
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id").eqNullSafe(F.col("d.id")), "inner")
    .select(
        "s.*",
        F.col("d.amount"),
        F.col("d.status").alias("order_status"),
        F.col("d.shipping_fee"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        "days_since_delivered_date",
        "days_delivered_date_to_shipped_date",
        "amount",
        "order_status",
        F.dense_rank().over(w).alias("cost_dense_rank"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

latest = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.col("o.status").alias("order_status"),
        F.col("src.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.days_since_delivered_date")).alias("max_days_since_delivered_date"),
    )
    .orderBy(F.col("order_status").desc_nulls_last())
)
