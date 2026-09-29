from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["North", "West"]) & (F.year("opened_date") == 2025) & F.col("region").isin(["North", "Central"]))
    .select(
        "opened_date",
        "region",
        F.concat_ws("-", "store_name", "region").alias("store_name_region_key"),
    )
    .orderBy(F.col("opened_date").desc())
)
