from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3, 5]) & F.col("product_id").isin([8, 9]))
    .groupBy("product_id", "store_id", F.year("sale_date").alias("sale_year"))
    .agg(
        F.count("units").alias("units_count"),
        F.count("*").alias("row_count"),
        F.max("sale_date").alias("last_sale_date"),
    )
    .filter(F.col("units_count") < 5)
    .orderBy("units_count", "sale_year")
)
