from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("store_name_length").desc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())

cleaned = (
    df_stores
    .filter(F.col("region").eqNullSafe(F.lit("Central")))
    .select(
        "id",
        "store_name",
        "city",
        F.length("store_name").alias("store_name_length"),
        F.coalesce("region", F.lit("OTHER")).alias("region_filled"),
    )
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest_match = (
    ranked
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.store_name_length",
        "s.rank_in_group",
        F.col("c.units").alias("store_sale_units"),
    )
)

base = (
    latest_match
    .filter(F.col("store_sale_units") <= 18)
    .select(
        "id",
        "store_name",
        "store_name_length",
        "store_sale_units",
        F.ceil("store_sale_units").alias("store_sale_units_whole"),
    )
)

grouped = (
    base
    .groupBy(
        F.when(F.col("store_sale_units") >= 43, "HIGH").otherwise("LOW").alias("store_sale_units_tier"),
        F.when(F.col("store_sale_units_whole") >= 80, "LARGE").otherwise("SMALL").alias("store_sale_units_whole_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("cnt"),
        F.percentile_approx("store_sale_units", 0.5).alias("store_sale_units_p50"),
    )
    .filter(F.col("cnt") > 1)
)

result = (
    grouped
    .select(
        "store_sale_units_tier",
        "store_sale_units_whole_tier",
        "cnt",
        "store_sale_units_p50",
    )
    .orderBy(F.col("store_sale_units_whole_tier").asc_nulls_first())
)
