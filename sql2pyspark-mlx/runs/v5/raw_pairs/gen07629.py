from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(144, 211))
    .groupBy(F.year("last_restock").alias("last_restock_year"))
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("max_quantity") > 90)
)
