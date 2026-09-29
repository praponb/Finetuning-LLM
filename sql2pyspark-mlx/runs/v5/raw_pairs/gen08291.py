from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit"),
        F.date_format("sale_date", "yyyy-MM").alias("sale_date_label"),
    )
)
