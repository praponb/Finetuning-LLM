from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "carrier",
        "shipped_date",
        "cost",
        F.substring("carrier", 1, 3).alias("carrier_prefix"),
    )
    .orderBy(F.col("cost").desc(), "shipped_date")
)
