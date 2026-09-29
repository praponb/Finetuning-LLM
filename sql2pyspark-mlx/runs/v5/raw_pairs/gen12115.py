from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

cleaned = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-08-10").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.coalesce("store_name", F.lit("N/A")).alias("store_name_filled"),
        F.concat_ws(",", "city", "region").alias("city_region_key"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock"))
)

filtered = (
    with_last
    .filter(F.col("region").isNotNull() & (~F.col("region").eqNullSafe(F.lit("Central"))))
    .select(
        "*",
        F.date_trunc("MONTH", "last_restock").alias("last_restock_period"),
        F.coalesce("region", F.lit("N/A")).alias("region_filled"),
    )
)

filtered_stores = (
    filtered
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-02-26"))) & F.col("region").isNotNull())
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
    .withColumn("region_clean", F.coalesce("region", F.lit("UNKNOWN")))
)

result = (
    filtered_stores
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        "city_region_key",
        "last_restock",
        "last_restock_period",
        "region_filled",
        "opened_date_month_end",
    )
    .orderBy(F.col("id").asc_nulls_last())
)
