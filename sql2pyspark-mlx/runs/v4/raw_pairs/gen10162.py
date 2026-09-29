from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
    )
)

result = (
    base
    .select("id", "order_id", "line_total", "unit_price")
    .unpivot(["id", "order_id"], ["line_total", "unit_price"], "kpi", "metric_value")
    .filter(F.col("metric_value").isNotNull())
)
