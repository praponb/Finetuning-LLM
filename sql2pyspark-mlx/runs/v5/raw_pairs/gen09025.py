from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 4, 6, 9]) & (F.col("product_id").isin([7, 8]) | F.col("revenue").between(1900, 2400)))
    .select(
        "id",
        "revenue",
        "units",
        "store_id",
        F.ceil("units").alias("units_whole"),
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)
