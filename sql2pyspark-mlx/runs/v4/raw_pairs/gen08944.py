from pyspark.sql import functions as F

result = (
    df_products
    .select("price", F.lit("product").alias("source"))
    .unionByName(
        df_shipments
        .select(F.col("cost").alias("price"), F.lit("shipment").alias("source"))
    )
    .distinct()
)
