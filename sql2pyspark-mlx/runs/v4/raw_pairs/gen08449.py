from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        F.last_day("sale_date").alias("sale_date_month_end"),
        F.col("revenue").cast("int").alias("revenue_int"),
    )
)
