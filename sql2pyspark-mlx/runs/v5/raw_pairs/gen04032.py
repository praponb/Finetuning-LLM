from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~((F.year("delivered_date") == 2024) | (F.col("carrier") == "DHL")))
    .select(
        "carrier",
        F.ceil("cost").alias("cost_whole"),
        F.lower("carrier").alias("carrier_lower"),
    )
    .orderBy("carrier_lower")
)
