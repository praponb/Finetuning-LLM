from pyspark.sql import functions as F

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("unit_price").between(190, 250))
    .withColumn("quantity_str", F.col("quantity").cast("double"))
)

result = (
    filtered
    .select("id", "order_id", "unit_price", F.col("quantity_str").alias("QUANTITY_STR"))
    .unpivot(
        ids=["id", "order_id"],
        values=["unit_price", "QUANTITY_STR"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
)
