from pyspark.sql import functions as F

cleaned = (
    df_order_items
    .filter(F.col("quantity") >= 5)
    .select(
        "*",
        F.round((F.col("quantity") * 100), 2).alias("quantity_adjusted"),
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
)

result = (
    cleaned
    .select("id", "product_id", "unit_price", "quantity_adjusted")
    .unpivot(
        ids=["id", "product_id"],
        values=["unit_price", "quantity_adjusted"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
