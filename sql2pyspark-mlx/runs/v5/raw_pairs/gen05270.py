from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["RETURNED", "IN_TRANSIT"]) & (F.col("shipped_date") <= "2025-04-01"))
    .select(
        "delivered_date",
        "carrier",
        "cost",
        F.initcap(F.trim("carrier")).alias("carrier_title"),
    )
)
