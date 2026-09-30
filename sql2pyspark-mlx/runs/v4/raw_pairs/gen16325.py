from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .select("delivered_date")
    .intersect(df_products.select(F.col("launch_date").alias("delivered_date")))
    .orderBy("delivered_date")
)
