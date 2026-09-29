from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2024-04-12").cast("date")) & F.col("order_id").isNotNull() & F.col("shipped_date").between("2023-07-01", "2024-07-09"))
    .withColumn("delivered_date_dayofweek", F.dayofweek("delivered_date"))
)

cleaned_shipments = (
    cleaned
    .filter(F.col("carrier").isNotNull() & (F.col("cost") >= 41))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "delivered_date_dayofweek",
        F.substring("carrier", 1, 3).alias("carrier_prefix"),
        F.concat_ws("-", "status", "carrier").alias("status_carrier_key"),
    )
)

with_dims = (
    cleaned_shipments
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select(
        "s.*",
        F.col("d.discount"),
        F.col("d.status").alias("order_status"),
        F.col("d.amount"),
    )
)

enriched = (
    with_dims
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.status").alias("order_status_2"))
)

enriched_shipments = (
    enriched
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.shipping_fee"))
)

result = (
    enriched_shipments
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "delivered_date_dayofweek",
        "status_carrier_key",
        "discount",
        "amount",
    )
    .orderBy(F.col("delivered_date_dayofweek").asc_nulls_last())
)
