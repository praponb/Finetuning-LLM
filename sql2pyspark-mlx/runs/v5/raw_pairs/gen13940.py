from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([14, 24]))
    .select("unit_price")
    .unionByName(
        df_shipments
        .filter(F.col("order_id").isNull())
        .select(F.col("cost").alias("unit_price"))
    )
    .distinct()
)
