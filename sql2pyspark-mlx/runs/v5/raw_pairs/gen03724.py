from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy(F.col("last_known_cost").desc())

with_windows = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "DELIVERED", "LOST"]))
    .select(
        "*",
        F.avg("cost").over(w).alias("cost_avg_5"),
        F.last("cost", ignorenulls=True).over(w2).alias("last_known_cost"),
    )
)

cleaned = (
    with_windows
    .filter((F.col("carrier").eqNullSafe(F.lit("UPS"))) & F.col("carrier").isNotNull())
    .select("*", F.col("cost").cast("int").alias("cost_int"))
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
)

with_dims = (
    ranked
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .select(
        "s.id",
        "s.order_id",
        "s.shipped_date",
        "s.delivered_date",
        "s.cost",
        "s.status",
        "s.cost_avg_5",
        "s.last_known_cost",
        "s.latest_rank",
        F.col("b.status").alias("order_status"),
        F.col("b.channel"),
        F.col("b.discount"),
    )
)

result = (
    with_dims
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .groupingSets(
        [["order_channel", "order_status", "shipment_status"], ["order_channel", "order_status"], ["order_channel"]],
        F.col("d.channel").alias("order_channel"),
        F.col("d.status").alias("order_status"),
        F.col("a.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("a.last_known_cost")), 2).alias("total_last_known_cost"),
        F.max(F.col("a.cost")).alias("max_cost"),
    )
)
