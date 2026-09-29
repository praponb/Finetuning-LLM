from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") != 27) & (F.col("revenue") > 3600))
    .select(
        "store_id",
        "revenue",
        "sale_date",
        F.coalesce("revenue", F.lit(0)).alias("revenue_filled"),
        F.date_sub("sale_date", 11).alias("sale_date_minus_11d"),
    )
)
