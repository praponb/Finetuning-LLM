from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") > "2023-12-21")
    .groupBy(
        F.date_trunc("MONTH", "last_restock").alias("last_restock_month"),
        "store_id",
    )
    .agg(
        F.max("last_restock").alias("last_last_restock"),
        F.max("last_restock").alias("latest_last_restock"),
        F.countDistinct("quantity").alias("unique_products"),
    )
    .orderBy("last_restock_month", "store_id")
    .limit(10)
)
