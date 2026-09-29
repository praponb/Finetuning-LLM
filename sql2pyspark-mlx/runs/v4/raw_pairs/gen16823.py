from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date", F.lit("product").alias("source"))
    .unionByName(
        df_shipments
        .filter(F.col("order_id").isNull())
        .select(
        F.col("shipped_date").alias("launch_date"),
        F.lit("shipment").alias("source"),
    )
    )
)
