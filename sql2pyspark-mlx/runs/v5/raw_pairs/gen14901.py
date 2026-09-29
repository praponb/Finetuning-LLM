from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") == "South") & F.upper("store_name").like("%ALL%") & F.col("region").isin(["Central", "East", "West"]))
    .select(
        "id",
        "opened_date",
        "region",
        "store_name",
        F.length("region").alias("region_length"),
    )
    .orderBy("region")
)
