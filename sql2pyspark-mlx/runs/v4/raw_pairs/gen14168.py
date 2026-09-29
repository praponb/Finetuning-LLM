from pyspark.sql import functions as F

valid = (
    df_shipments
    .filter(F.col("cost").between(26, 64))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0).alias("is_in_transit"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_clean"),
    )
)

result = (
    valid
    .groupBy("order_id")
    .pivot("status", ["IN_TRANSIT", "LOST", "RETURNED"])
    .agg(F.avg("cost"))
)
