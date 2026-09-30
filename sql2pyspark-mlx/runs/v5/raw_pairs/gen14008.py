from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("quantity").isNotNull())
    .withColumn("line_total", (F.col("quantity") * F.col("unit_price")))
)

result = (
    base
    .select(
        "id",
        "product_id",
        "line_total",
        F.col("unit_price").alias("UNIT_PRICE_VAL"),
    )
    .unpivot(
        ids=["id", "product_id"],
        values=["line_total", "UNIT_PRICE_VAL"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
