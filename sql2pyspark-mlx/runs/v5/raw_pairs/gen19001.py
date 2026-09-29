from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3]))
    .groupBy(
        F.date_trunc("MONTH", "last_restock").alias("last_restock_month"),
        "store_id",
    )
    .agg(F.count("quantity").alias("quantity_count"))
    .filter(F.col("quantity_count") < 9)
    .orderBy(F.col("last_restock_month").desc(), F.col("quantity_count").desc())
)
