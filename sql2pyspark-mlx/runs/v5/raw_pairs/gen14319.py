from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("units").desc(), "id")
w2 = Window.partitionBy("store_id", "region").orderBy(F.col("sqft").asc_nulls_first(), "id")
w3 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w4 = Window.orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_store_sales
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_id", "revenue")
)

enriched = (
    ranked
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "inner")
    .select("s.id", "s.store_id", F.col("p2.city"))
)

filtered = (
    enriched
    .filter(F.col("city") == "Tokyo")
    .select(
        "*",
        F.initcap(F.trim("city")).alias("city_title"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

enriched_store_sales = (
    filtered
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.city").isin(["Tokyo", "Singapore"]))
    .select("e.*", F.col("s.opened_date"), F.col("s.sqft"), F.col("s.region"))
)

base = (
    enriched_store_sales
    .filter((F.col("city").eqNullSafe(F.lit("Bangkok"))) & F.col("store_id").isin([3, 6]))
    .select(
        "id",
        "store_id",
        "city_title",
        "resolved_city",
        "sqft",
        "region",
        F.lower("city").alias("city_lower"),
    )
)

ranked_store_sales = (
    base
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    ranked_store_sales
    .select(
        "id",
        "resolved_city",
        F.row_number().over(w3).alias("row_seq"),
        F.sum("sqft").over(w4).alias("cumulative_sqft"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("cumulative_sqft") >= 77371, "LARGE").otherwise("SMALL").alias("cumulative_sqft_tier"),
    )
    .agg(
        F.percentile_approx("row_seq", 0.5).alias("row_seq_p50"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("cumulative_sqft"), 2).alias("avg_cumulative_sqft"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
        F.percentile_approx("row_seq", 0.5).alias("row_seq_p50_2"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("avg_cumulative_sqft").desc_nulls_first())
)
