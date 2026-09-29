from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.avg("units").alias("avg_units"),
        F.max("sale_date").alias("last_sale_date"),
    )
    .filter(F.col("avg_units") > 33)
    .select("store_id", "avg_units", "last_sale_date")
)
