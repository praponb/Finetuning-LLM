from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "quantity",
        F.round((F.col("quantity") * 0.9), 2).alias("quantity_scaled"),
    )
    .orderBy("quantity_scaled", "id")
    .limit(10)
)
