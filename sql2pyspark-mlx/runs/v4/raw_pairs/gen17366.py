from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").isNotNull() & (F.col("store_id") == 6) & (F.col("revenue") <= 1500))
    .select(
        "store_id",
        "units",
        "revenue",
        F.floor("revenue").alias("revenue_whole"),
        F.date_sub("sale_date", 35).alias("sale_date_minus_35d"),
    )
    .orderBy(F.col("store_id").desc())
)
