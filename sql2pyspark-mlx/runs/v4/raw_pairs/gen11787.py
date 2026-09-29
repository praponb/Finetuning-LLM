from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("revenue") < 1500)
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit"),
    )
)

result = (
    base
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "left")
    .select("src.*", F.col("s.store_name"), F.col("s.sqft"))
)
