from pyspark.sql import functions as F

cleaned = (
    df_order_items
    .filter(F.col("unit_price") >= 170)
    .select(
        "*",
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
    )
)

result = (
    cleaned
    .select("id", "order_id", "unit_price", "line_total")
    .unpivot(
        ids=["id", "order_id"],
        values=["unit_price", "line_total"],
        variableColumnName="kpi",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
