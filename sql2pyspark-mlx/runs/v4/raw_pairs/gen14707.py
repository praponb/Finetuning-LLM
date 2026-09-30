from pyspark.sql import functions as F

base = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-11-15"))) & F.col("sqft").between(12841, 14129))
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        F.lower("region").alias("region_lower"),
        F.ceil("sqft").alias("sqft_whole"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_filled"),
    )
)

result = (
    base
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.amount") >= 1600), "left_semi")
)
