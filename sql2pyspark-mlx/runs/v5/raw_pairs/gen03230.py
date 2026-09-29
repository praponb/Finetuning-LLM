from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3, 4, 6]) & (F.col("store_id") == 2))
    .withColumn("sale_date_plus_months", F.add_months("sale_date", 12))
)

store_id_summary = (
    base
    .groupBy("store_id", "product_id")
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
    )
)

result = store_id_summary.orderBy(F.col("store_id").desc())
