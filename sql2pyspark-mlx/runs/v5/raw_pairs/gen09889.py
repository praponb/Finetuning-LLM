from pyspark.sql import functions as F

result = (
    df_inventory
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.sum("quantity").alias("total_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .orderBy("total_quantity")
)
