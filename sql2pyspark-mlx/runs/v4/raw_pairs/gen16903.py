from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") != 171) | F.col("product_id").isin([1, 5, 6, 10]))
    .select(
        "id",
        "product_id",
        "quantity",
        F.round("quantity", 2).alias("quantity_rounded"),
    )
    .orderBy("product_id", F.col("quantity").desc())
)
