from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("product_id") == 5)

store_id_summary = (
    base
    .groupBy("store_id")
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
)

result = (
    store_id_summary
    .filter(F.col("avg_units") >= 58)
    .select("store_id", "first_sale_date", "avg_units")
)
