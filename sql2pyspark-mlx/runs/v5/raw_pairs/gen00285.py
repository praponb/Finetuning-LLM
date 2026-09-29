from pyspark.sql import functions as F

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "unit_price",
        F.round("quantity", 1).alias("quantity_rounded"),
        F.when(F.col("quantity") >= 7, "High").when(F.col("quantity") >= 4, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

result = (
    filtered
    .select(
        "id",
        "order_id",
        F.col("quantity_rounded").alias("QUANTITY_ROUNDED"),
        "unit_price",
    )
    .unpivot(
        ids=["id", "order_id"],
        values=["QUANTITY_ROUNDED", "unit_price"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
