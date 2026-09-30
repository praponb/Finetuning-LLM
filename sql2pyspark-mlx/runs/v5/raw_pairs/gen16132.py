from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date_period", "id")
w2 = Window.partitionBy("status").orderBy("shipped_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("shipped_date_period", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("cost_filled", "id")
w5 = Window.orderBy("shipped_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-06-02").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "*",
        F.concat("carrier", F.lit("/"), "status").alias("carrier_status"),
        F.coalesce("carrier", F.lit("OTHER")).alias("resolved_carrier"),
    )
)

cleaned = (
    filtered
    .filter((F.col("delivered_date") > F.lit("2023-08-29").cast("date")) & F.col("carrier").isNotNull())
    .withColumn("cost_filled", F.coalesce("cost", F.lit(0)))
    .withColumn("shipped_date_period", F.date_trunc("YEAR", "shipped_date"))
)

with_prev = cleaned.withColumn("prev_shipped_date_period", F.lag("shipped_date_period").over(w))

gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_shipped_date_period").isNull() | (F.col("shipped_date_period") > F.col("prev_shipped_date_period") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

with_windows = (
    sessionized
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "carrier_status",
        "prev_shipped_date_period",
        "island_id",
        F.avg("island_id").over(w3).alias("island_id_avg_5"),
        F.row_number().over(w4).alias("row_seq"),
        F.sum("cost_filled").over(w5).alias("running_cost_filled"),
    )
)

result = (
    with_windows
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .rollup(
        F.col("d.channel").alias("channel"),
        F.col("s.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("s.row_seq")), 2).alias("total_row_seq"),
        F.min(F.col("s.island_id")).alias("min_island_id"),
    )
)
