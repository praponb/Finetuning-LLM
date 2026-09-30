from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~((F.col("quantity") < 126) & F.month("last_restock").isin([3, 6, 10])))
    .select(
        "last_restock",
        "quantity",
        F.when(F.col("quantity") >= 192, "High").when(F.col("quantity") >= 74, "Medium").otherwise("Low").alias("quantity_band"),
    )
)
