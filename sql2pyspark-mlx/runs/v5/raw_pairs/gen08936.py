from pyspark.sql import functions as F

result = (
    df_shipments
    .select("delivered_date")
    .unionByName(df_store_sales.select(F.col("sale_date").alias("delivered_date")))
)
