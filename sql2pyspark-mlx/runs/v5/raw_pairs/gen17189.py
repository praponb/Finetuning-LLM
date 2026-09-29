from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price_whole").desc(), "id")
w2 = Window.orderBy(F.col("unit_price").desc(), "id")
w3 = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy(F.col("running_unit_price_whole").desc(), "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isin([9, 11, 29, 32]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.round((F.col("unit_price") * 1.1), 2).alias("unit_price_adjusted"),
    )
)

filtered_order_items = (
    filtered
    .filter(F.col("product_id").isNull())
    .select("*", F.floor("unit_price").alias("unit_price_whole"))
)

latest = (
    filtered_order_items
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_windows = (
    latest
    .filter(F.col("quantity").between(4, 5))
    .select(
        "*",
        F.ntile(4).over(w2).alias("unit_price_bucket"),
        F.sum("unit_price_whole").over(w3).alias("running_unit_price_whole"),
    )
)

deduped = (
    with_windows
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .alias("s")
    .join(df_products.alias("dim"), F.col("s.product_id") == F.col("dim.id"), "inner")
    .groupBy(F.col("dim.brand").alias("brand"))
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("s.running_unit_price_whole")).alias("avg_running_unit_price_whole"),
        F.avg(F.col("s.unit_price_adjusted")).alias("avg_unit_price_adjusted"),
    )
)
