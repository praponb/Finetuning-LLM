from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        F.round((F.col("quantity") * 0.85), 2).alias("quantity_adjusted"),
        F.round((F.col("quantity") * 0.85), 2).alias("quantity_adjusted_2"),
    )
    .orderBy("quantity_adjusted", "id")
    .limit(10)
)
