from pyspark.sql import functions as F

result = (
    df_orders
    .select(F.lit("order").alias("origin"), "order_date")
    .unionByName(
        df_shipments
        .filter(F.col("status").isin(["LOST", "IN_TRANSIT"]))
        .select(
        F.lit("shipment").alias("origin"),
        F.col("shipped_date").alias("order_date"),
    )
    )
)
