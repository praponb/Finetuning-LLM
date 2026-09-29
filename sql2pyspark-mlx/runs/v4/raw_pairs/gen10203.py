from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "Kerry")
    .select(
        "cost",
        "shipped_date",
        F.lower("status").alias("status_lower"),
        F.substring("carrier", 1, 4).alias("carrier_prefix"),
    )
)
