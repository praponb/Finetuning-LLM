from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(2100, 2800))
    .groupBy(F.year("sale_date").alias("sale_year"), "product_id")
    .agg(F.min("units").alias("min_units"))
    .filter(F.col("min_units") < 27)
)
