from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("line_total", (F.col("unit_price") - F.col("quantity")))
)

result = (
    base
    .select(
        "id",
        "order_id",
        F.col("unit_price").alias("UNIT_PRICE_VAL"),
        F.col("line_total").alias("LINE_TOTAL_VAL"),
    )
    .unpivot(
        ids=["id", "order_id"],
        values=["UNIT_PRICE_VAL", "LINE_TOTAL_VAL"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
)
