from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 7, 9]) & ((F.col("sale_date") >= "2024-03-05") | (F.col("units") <= 14)))
)

summary = (
    base
    .groupBy(F.year("sale_date").alias("sale_year"), "store_id")
    .agg(
        F.min("units").alias("min_units"),
        F.min("sale_date").alias("min_sale_date"),
    )
)

result = summary.filter(F.col("min_units") >= 20)
