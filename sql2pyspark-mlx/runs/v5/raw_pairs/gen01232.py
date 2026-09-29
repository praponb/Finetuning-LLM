from pyspark.sql import functions as F

result = (
    df_products
    .select(F.lit("product").alias("source"), "price")
    .union(
        df_shipments
        .filter(F.col("order_id").isNotNull())
        .select(F.lit("shipment").alias("source"), "cost")
    )
    .distinct()
    .orderBy("price")
)
