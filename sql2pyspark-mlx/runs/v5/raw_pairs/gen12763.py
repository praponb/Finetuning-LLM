from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 5)
    .groupBy(
        "store_id",
        F.date_format("sale_date", "yyyy-MM").alias("sale_month"),
        "product_id",
    )
    .agg(F.min("units").alias("min_units"))
    .filter(F.col("min_units") > 35)
)
