from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("region").orderBy(F.col("store_sale_units").asc_nulls_last(), "id")

valid = (
    df_stores
    .filter(F.col("store_name").like("%Rive%"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.round("sqft", 2).alias("sqft_rounded"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_clean"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.city",
        "s.region",
        "s.sqft_rounded",
        "s.region_clean",
        F.col("c.units"),
        F.col("c.revenue").alias("store_sale_revenue"),
    )
)

with_last = (
    latest_match
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.region",
        "s.sqft_rounded",
        "s.region_clean",
        "s.store_sale_revenue",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.units").alias("store_sale_units"),
    )
)

latest_match_stores = (
    with_last
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units"))
)

deduped = (
    latest_match_stores
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "store_name", "store_sale_revenue", "store_sale_units", "units")
)

result = (
    deduped
    .select(
        "id",
        "store_name",
        "store_sale_revenue",
        "store_sale_units",
        F.when(F.col("store_name").rlike("^(Old Town|Downtown Express)$"), "GROUP_A").when(F.col("store_name").rlike("^(Siam Flagship|Airport Kiosk)$"), "CORE").otherwise("PRIORITY").alias("store_name_zone"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
