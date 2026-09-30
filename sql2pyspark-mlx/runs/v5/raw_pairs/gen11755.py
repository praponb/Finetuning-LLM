from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 5]) & (F.col("units") <= 23))
    .select(
        "id",
        "sale_date",
        F.date_format("sale_date", "MMM yyyy").alias("sale_date_label"),
    )
)
