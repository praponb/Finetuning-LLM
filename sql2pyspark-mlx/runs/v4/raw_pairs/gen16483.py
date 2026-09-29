from pyspark.sql import functions as F

result = (
    df_orders
    .select("status", F.lit("order").alias("source"))
    .unionByName(
        df_shipments
        .filter(F.col("delivered_date") > "2024-06-01")
        .select("status", F.lit("shipment").alias("source"))
    )
    .orderBy(F.col("status").desc())
)
