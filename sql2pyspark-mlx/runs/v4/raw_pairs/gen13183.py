from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") <= 61) | (F.col("carrier") == "FedEx"))
    .select(
        "order_id",
        "status",
        F.coalesce("carrier", F.lit("unassigned")).alias("carrier_filled"),
    )
    .orderBy(F.col("order_id").desc(), "status")
)
