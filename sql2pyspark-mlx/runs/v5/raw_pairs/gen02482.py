from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") < "2024-05-01")
    .select(
        "id",
        "delivered_date",
        "status",
        F.initcap(F.trim("carrier")).alias("carrier_title"),
    )
)
