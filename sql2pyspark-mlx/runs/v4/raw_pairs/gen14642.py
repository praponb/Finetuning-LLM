from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "status",
        "cost",
        F.initcap(F.trim("carrier")).alias("carrier_title"),
    )
)
