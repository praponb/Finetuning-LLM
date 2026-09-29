from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 4, 6]) | (F.col("revenue") > 3300))
    .groupBy("product_id")
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        F.avg("units").alias("avg_units"),
    )
    .filter(F.col("avg_units") >= 26)
    .orderBy("avg_units")
)
