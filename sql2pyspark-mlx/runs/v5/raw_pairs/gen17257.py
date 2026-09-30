from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("product_id", "order_id").orderBy(F.col("unit_price_rounded").desc())
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.avg("unit_price").alias("avg_order_item_unit_price"))

cleaned = (
    df_order_items
    .filter(F.col("order_id") == 27)
    .withColumn("unit_price_rounded", F.round("unit_price", 0))
)

metrics = (
    cleaned
    .filter(F.col("quantity").between(4, 7))
    .select(
        "*",
        F.count("quantity").over(w_ordered).alias("last3_count_quantity"),
        F.min("quantity").over(w_ordered).alias("last3_min_quantity"),
        F.avg("quantity").over(w_ordered).alias("last3_avg_quantity"),
        F.sum("quantity").over(w_ordered).alias("last3_sum_quantity"),
        F.dense_rank().over(w2).alias("unit_price_rounded_dense_rank"),
    )
)

base = (
    metrics
    .filter(F.col("product_id").isNotNull() & (F.col("unit_price_rounded_dense_rank") >= 7))
    .withColumn("quantity_scaled", F.round((F.col("quantity") * 0.9), 2))
)

joined = (
    base
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.status"), F.col("d.amount"))
)

result = (
    joined
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "unit_price_rounded",
        "last3_count_quantity",
        "last3_min_quantity",
        "last3_avg_quantity",
        F.col("avg_order_item_unit_price"),
    )
)
