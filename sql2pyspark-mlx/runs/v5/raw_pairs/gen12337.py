from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date_period", "id")
w2 = Window.partitionBy("status").orderBy("delivered_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy(F.col("delivered_date").desc(), "id")

filtered = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (F.col("shipped_date") >= F.to_date(F.lit("2023-09-10"))))
    .withColumn("delivered_date_period", F.date_trunc("YEAR", "delivered_date"))
)

with_gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("delivered_date_period").over(w).isNull() | (F.col("delivered_date_period") > F.lag("delivered_date_period").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_shipments = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

enriched = (
    sessions_shipments
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.customer_id").isin([3, 17, 31]))
    .select(
        "e.id",
        "e.order_id",
        "e.carrier",
        "e.shipped_date",
        "e.delivered_date",
        "e.cost",
        "e.delivered_date_period",
        F.col("o.shipping_fee"),
        F.col("o.order_date"),
    )
)

joined = (
    enriched
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.discount"), F.col("p.status"))
)

ranked = (
    joined
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .alias("e")
    .join(df_orders.alias("dim"), F.col("e.order_id") == F.col("dim.id"), "left")
    .groupBy(
        F.col("dim.channel").alias("channel"),
        F.col("e.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("e.shipping_fee")).alias("min_shipping_fee"),
        F.max(F.col("e.cost")).alias("max_cost"),
    )
)
