from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .pivot("carrier", ["DHL", "Kerry"])
    .agg(F.sum("cost").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
