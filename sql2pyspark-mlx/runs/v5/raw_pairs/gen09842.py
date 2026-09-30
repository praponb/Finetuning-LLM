from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id", "product_id")
    .agg(
        F.count("*").alias("num_inventory"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .filter(F.col("quantity_range") > 79)
    .select("store_id", "product_id", "num_inventory", "quantity_range")
)
