from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") != "DELIVERED")
    .select(
        "shipped_date",
        "delivered_date",
        F.initcap(F.trim("status")).alias("status_title"),
    )
)
