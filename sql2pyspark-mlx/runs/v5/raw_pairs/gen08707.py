from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("quantity", "id")

filtered = (
    df_order_items
    .filter(F.col("product_id") == 2)
    .select("*", (F.col("unit_price") + F.col("quantity")).alias("line_total"))
)

cleaned = (
    filtered
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([12, 22, 30, 35]))
    .select(
        "*",
        (F.col("line_total") - F.col("quantity")).alias("line_total_minus_quantity"),
    )
)

with_dims = (
    cleaned
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.discount") >= 0.17)
    .select(
        "src.id",
        "src.order_id",
        "src.product_id",
        "src.quantity",
        "src.unit_price",
        "src.line_total_minus_quantity",
        F.col("b.status"),
    )
)

latest = (
    with_dims
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "order_id", "product_id", "status")
)

result = (
    latest
    .groupBy("order_id", "status")
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.count_if(F.col("status") == "PENDING").alias("pending_events"),
    )
)
