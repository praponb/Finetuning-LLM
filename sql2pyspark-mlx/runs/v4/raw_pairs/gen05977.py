from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2024-04-16").cast("date"))
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        F.when(F.col("units") >= 41, "High").when(F.col("units") >= 12, "Medium").otherwise("Low").alias("units_level"),
    )
)

joined = (
    base
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.city"), F.col("d.opened_date"))
)

with_dims = (
    joined
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.city").alias("store_city"))
)

valid = (
    with_dims
    .filter((F.col("sale_date") > F.lit("2024-01-01").cast("date")) & F.col("store_city").isNotNull())
    .select(
        "id",
        "units",
        "revenue",
        "units_level",
        "opened_date",
        "store_city",
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)

daily = (
    valid
    .groupBy(
        F.when(F.col("revenue") >= 3300, "LARGE").otherwise("SMALL").alias("revenue_tier"),
    )
    .agg(
        F.stddev_samp("units").alias("units_stddev"),
        F.percentile_approx("revenue", 0.9).alias("revenue_p90"),
        F.count(F.when(F.col("store_city") == "Chiang Mai", 1)).alias("chiang_mai_rows"),
        F.avg(F.when(F.col("store_city") == "Tokyo", F.col("revenue"))).alias("tokyo_avg_revenue"),
        F.sort_array(F.collect_set("store_city")).alias("store_city_list"),
        F.sort_array(F.collect_set("store_city")).alias("store_city_list_2"),
    )
)

result = (
    daily
    .select(
        "revenue_tier",
        "units_stddev",
        "revenue_p90",
        "tokyo_avg_revenue",
        "store_city_list",
        "store_city_list_2",
    )
    .orderBy(F.col("revenue_tier").desc())
)
