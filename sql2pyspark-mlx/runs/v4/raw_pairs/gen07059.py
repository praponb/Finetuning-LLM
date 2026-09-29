from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 5, 6]))
    .groupBy("store_id", "product_id")
    .agg(
        F.sum("units").alias("total_units"),
        F.min("sale_date").alias("min_sale_date"),
    )
    .filter(F.col("total_units") < 230)
    .select("store_id", "product_id", "total_units", "min_sale_date")
    .orderBy(F.col("min_sale_date").desc(), F.col("product_id").desc(), "store_id")
    .limit(5)
)
