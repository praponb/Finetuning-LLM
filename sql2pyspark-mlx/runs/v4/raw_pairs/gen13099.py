from pyspark.sql import functions as F

result = (
    df_products
    .select(F.col("price").alias("value"))
    .unionByName(
        df_shipments
        .filter(F.col("order_id").isNull())
        .select(F.col("cost").alias("value"))
    )
)
