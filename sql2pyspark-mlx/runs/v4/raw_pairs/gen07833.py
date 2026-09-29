from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 4]))
    .select(
        "id",
        "sale_date",
        "store_id",
        F.floor("revenue").alias("revenue_whole"),
        F.coalesce("revenue", F.lit(0)).alias("revenue_filled"),
    )
    .orderBy(F.col("sale_date").desc(), "id")
    .limit(10)
)
