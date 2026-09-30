from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("status", "order_id")
w3 = Window.partitionBy("carrier").orderBy(F.col("shipped_date").asc_nulls_first(), "id")
w4 = Window.partitionBy("carrier", "order_id").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("cost", "id")

with_windows = (
    df_shipments
    .select(
        "*",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.avg("cost").over(w2).alias("status_avg_cost"),
    )
)

deduped = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

joined = (
    deduped
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.store_id").isNotNull())
    .select("s.*", F.col("b.amount"))
)

with_windows_shipments = (
    joined
    .select(
        "id",
        "order_id",
        "shipped_date",
        "status",
        "cost_share",
        "latest_rank",
        F.last("amount", ignorenulls=True).over(w4).alias("last_known_amount"),
        F.ntile(3).over(w5).alias("cost_bucket"),
    )
)

result = (
    with_windows_shipments
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .cube(
        F.col("o.channel").alias("channel"),
        F.col("src.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("src.cost_share")), 2).alias("avg_cost_share"),
        F.grouping_id().alias("level_id"),
    )
)
