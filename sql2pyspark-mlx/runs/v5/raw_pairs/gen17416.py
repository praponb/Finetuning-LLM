from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("pending_count").desc())

joined = (
    df_order_items
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.product_id",
        "src.quantity",
        "src.unit_price",
        F.col("p.status"),
    )
)

result = (
    joined
    .groupBy("status", "order_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_count"),
    )
    .withColumn("pending_count_rank", F.rank().over(w))
    .orderBy(F.col("status").asc_nulls_last())
)
