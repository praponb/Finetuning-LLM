from pyspark.sql import functions as F

result = (
    df_shipments
    .select("cost", F.lit("shipment").alias("source"))
    .unionByName(
        df_products
        .select(F.col("price").alias("cost"), F.lit("product").alias("source"))
    )
)
