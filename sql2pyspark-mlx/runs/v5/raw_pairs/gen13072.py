from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id") == 5)
    .select(F.col("sale_date").alias("event_date"))
    .unionByName(
        df_inventory
        .filter(F.col("store_id").isin([2, 4, 5]))
        .select(F.col("last_restock").alias("event_date"))
    )
    .orderBy("event_date")
)
