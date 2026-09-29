from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "delivered_date",
        "carrier",
        F.initcap(F.trim("status")).alias("status_title"),
    )
)
