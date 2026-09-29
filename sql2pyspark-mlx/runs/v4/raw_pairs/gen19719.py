from pyspark.sql import functions as F

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([6, 28]))
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
        F.when(F.col("unit_price") >= 440, "High").when(F.col("unit_price") >= 130, "Medium").otherwise("Low").alias("unit_price_band"),
    )
)

result = (
    filtered
    .select("id", "order_id", "unit_price", F.col("line_total").alias("LINE_TOTAL"))
    .unpivot(["id", "order_id"], ["unit_price", "LINE_TOTAL"], "metric_name", "val")
)
