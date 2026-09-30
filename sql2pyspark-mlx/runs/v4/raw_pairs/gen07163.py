from pyspark.sql import functions as F

cleaned = (
    df_order_items
    .filter(F.col("quantity") == 5)
    .select("*", (F.col("unit_price") - F.col("quantity")).alias("line_total"))
)

result = (
    cleaned
    .select("id", "product_id", "unit_price", "line_total")
    .unpivot(
        ["id", "product_id"],
        ["unit_price", "line_total"],
        "metric_name",
        "reading",
    )
)
