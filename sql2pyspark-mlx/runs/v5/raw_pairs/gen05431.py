from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") != "DELIVERED")
    .select(
        "id",
        "delivered_date",
        "cost",
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_filled"),
    )
)
