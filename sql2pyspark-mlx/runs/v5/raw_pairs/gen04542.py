from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(((F.col("units") <= 12) & (F.col("product_id") == 10)) | F.col("store_id").isin([2, 3, 5]))
    .select(
        "id",
        "revenue",
        "store_id",
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)
