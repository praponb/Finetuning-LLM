from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.avg("units").alias("avg_units"),
        F.max("revenue").alias("max_revenue"),
    )
    .orderBy("avg_units", "product_id")
    .limit(3)
)
