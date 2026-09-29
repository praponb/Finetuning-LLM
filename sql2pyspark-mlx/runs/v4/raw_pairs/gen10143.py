from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") < "2024-12-01")
    .select(
        "delivered_date",
        "carrier",
        "cost",
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
    )
)
