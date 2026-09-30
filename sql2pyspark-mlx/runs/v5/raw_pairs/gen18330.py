from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("line_total", "id").rowsBetween(-6, Window.currentRow)
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.max("unit_price").alias("max_order_item_unit_price"))

filtered = (
    df_order_items
    .filter(F.col("order_id") == 38)
    .select(
        "*",
        F.when(F.col("quantity") >= 8, "High").when(F.col("quantity") >= 5, "Medium").otherwise("Low").alias("quantity_band"),
        (F.col("quantity") - F.col("unit_price")).alias("line_total"),
    )
)

filtered_order_items = (
    filtered
    .filter(F.col("line_total") >= 42)
    .withColumn("quantity_minus_line_total", (F.col("quantity") - F.col("line_total")))
    .withColumn("greatest_unit_price_quantity", F.greatest("unit_price", "quantity"))
)

metrics = (
    filtered_order_items
    .select(
        "*",
        F.avg("unit_price").over(w).alias("last7_avg_unit_price"),
        F.min("unit_price").over(w).alias("last7_min_unit_price"),
        F.count("unit_price").over(w).alias("last7_count_unit_price"),
    )
)

base = (
    metrics
    .filter(F.col("product_id").isin([25, 30, 35]))
    .select(
        "*",
        F.when(F.col("quantity") >= 7, "High").when(F.col("quantity") >= 3, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

result = (
    base
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "quantity_band",
        "line_total",
        "quantity_minus_line_total",
        "greatest_unit_price_quantity",
        F.col("max_order_item_unit_price"),
    )
    .orderBy(F.col("unit_price").desc())
)
