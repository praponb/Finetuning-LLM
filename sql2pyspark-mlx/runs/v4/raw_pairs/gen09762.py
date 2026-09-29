from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(110, 140) & F.col("product_id").isin([1, 2]))
    .select(
        "id",
        "product_id",
        F.month("last_restock").alias("last_restock_month"),
        F.when(F.col("quantity") >= 199, "High").when(F.col("quantity") >= 91, "Medium").otherwise("Low").alias("quantity_level"),
    )
)
