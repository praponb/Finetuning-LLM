from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([31, 35]))
    .select("status", F.lit("order").alias("source"))
    .unionByName(
        df_shipments
        .filter(F.month("delivered_date").isin([8, 10, 11]))
        .select("status", F.lit("shipment").alias("source"))
    )
    .distinct()
    .orderBy("source")
)
