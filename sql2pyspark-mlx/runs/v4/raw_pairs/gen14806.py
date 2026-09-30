from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("order_id").isin([16, 25, 30]) & F.col("shipped_date").between("2023-03-01", "2025-08-29")) | F.col("carrier").isin(["Kerry", "UPS", "DHL"]))
    .select(
        "delivered_date",
        "order_id",
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
    )
)
