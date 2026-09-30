from pyspark.sql import functions as F

result = (
    df_shipments
    .select("cost", F.lit("shipment").alias("source"))
    .union(df_store_sales.select("revenue", F.lit("store_sale").alias("source")))
)
