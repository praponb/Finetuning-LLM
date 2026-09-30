from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") <= "2023-05-01")
    .select("cost", F.lit("shipment").alias("source"))
    .unionByName(
        df_order_items
        .filter(F.col("unit_price").isNull())
        .select(F.col("unit_price").alias("cost"), F.lit("order_item").alias("source"))
    )
    .orderBy(F.col("cost").desc())
)
