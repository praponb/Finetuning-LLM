from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

kept = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.store_id").isin([3, 4])), "left_anti")
)

latest_match = (
    kept
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.revenue"),
    )
)

matched_keys = (
    latest_match
    .select("id", "city")
    .subtract(
        df_stores
        .filter(F.col("city") == "Tokyo")
        .select("id", "city")
    )
)

reconciled = (
    latest_match
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("region").isNotNull())
    .select(
        "*",
        F.substring("region", 1, 2).alias("region_prefix"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_filled"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("revenue") >= 4100, "PREMIUM").otherwise("STANDARD").alias("revenue_tier"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Berlin", F.col("sqft")).otherwise(0)).alias("berlin_sqft"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.countDistinct("region").alias("distinct_regions"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.avg(F.when(F.col("region") == "South", F.col("sqft"))).alias("south_avg_sqft"),
        F.round(F.sum("revenue") / F.nullif(F.count(F.when(F.col("region") == "North", 1)), F.lit(0)), 2).alias("revenue_per_north"),
    )
    .filter(F.col("revenue_per_north") < 0)
    .orderBy(F.col("revenue_tier").desc())
)
