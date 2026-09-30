from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        F.when(F.col("quantity") >= 183, "High").when(F.col("quantity") >= 34, "Medium").otherwise("Low").alias("quantity_tier"),
    )
    .orderBy(F.col("id").desc())
)
