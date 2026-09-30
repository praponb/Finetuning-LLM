from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .select(F.col("delivered_date").alias("event_date"))
    .unionByName(
        df_store_sales
        .filter(F.col("store_id").isin([5, 6]))
        .select(F.col("sale_date").alias("event_date"))
    )
)
