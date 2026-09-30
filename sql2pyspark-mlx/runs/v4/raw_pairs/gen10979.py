from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-05-01"))) & F.col("product_id").isin([2, 4, 10]))
    .withColumn("revenue_per_unit", (F.col("units") * F.col("revenue")))
)

joined = (
    base
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.city"), F.col("p.store_name"))
)

joined_store_sales = (
    joined
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.store_name").alias("store_store_name"))
)

result = (
    joined_store_sales
    .cube(
        F.when(F.col("revenue_per_unit") >= 63, "LARGE").otherwise("SMALL").alias("revenue_per_unit_tier"),
    )
    .agg(
        F.percentile_approx("revenue", 0.95).alias("revenue_p95"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count_if(F.col("city") == "London").alias("london_events"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("revenue_per_unit")).otherwise(0)).alias("chiang_mai_revenue_per_unit"),
    )
    .orderBy(F.col("london_events").desc())
)
