from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        F.when(F.col("quantity") >= 8, "High").when(F.col("quantity") >= 2, "Medium").otherwise("Low").alias("quantity_level"),
    )
)

valid_order_items = (
    valid
    .filter(F.col("product_id").isin([2, 36, 37]))
    .withColumn("line_total", (F.col("unit_price") + F.col("quantity")))
)

scored = (
    valid_order_items
    .withColumn("order_id_count_line_total", F.count("line_total").over(w))
    .withColumn("line_total_avg_2", F.avg("line_total").over(w2))
)

with_dims = (
    scored
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.price"), F.col("p.stock_qty"))
)

result = (
    with_dims
    .groupBy("order_id")
    .agg(
        F.percentile_approx("unit_price", 0.9).alias("unit_price_p90"),
        F.avg("price").alias("avg_price"),
        F.round(F.avg("order_id_count_line_total"), 2).alias("avg_order_id_count_line_total"),
        F.count("*").alias("cnt"),
        F.round(F.avg("order_id_count_line_total"), 2).alias("avg_order_id_count_line_total_2"),
    )
    .orderBy(F.col("cnt").desc_nulls_first())
)
