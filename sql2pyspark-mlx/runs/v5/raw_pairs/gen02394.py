from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") <= 2500) & F.col("store_id").isin([4, 6]) & F.col("revenue").between(1200, 2800))
    .select(
        "units",
        "store_id",
        "revenue",
        F.date_format("sale_date", "yyyy-MM-dd").alias("sale_date_label"),
    )
)
