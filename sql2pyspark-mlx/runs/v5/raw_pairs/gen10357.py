from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") <= "2024-04-01") & F.col("store_id").isin([1, 3, 5, 6]))
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("last_restock_month"))
    .agg(
        F.max("last_restock").alias("latest_last_restock"),
        F.countDistinct("quantity").alias("unique_products"),
    )
    .filter(F.col("unique_products") >= 2)
    .orderBy(F.col("last_restock_month").desc())
)
