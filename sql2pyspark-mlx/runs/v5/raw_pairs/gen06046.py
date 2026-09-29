from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("sqft").asc_nulls_last())
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("region").orderBy("sqft", "id")
w5 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-2, Window.currentRow)
w7 = Window.partitionBy("city").orderBy(F.col("sqft_share").desc())

enriched = (
    df_stores
    .select(
        "*",
        F.first("region", ignorenulls=True).over(w).alias("first_known_region"),
        F.dense_rank().over(w2).alias("sqft_dense_rank"),
        F.first("sqft", ignorenulls=True).over(w).alias("first_known_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4).alias("sqft_share"),
    )
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_name", "city", "opened_date", "sqft", "sqft_share")
)

with_windows = (
    ranked
    .withColumn("cumulative_sqft", F.sum("sqft").over(w5))
    .withColumn("sqft_avg_2", F.avg("sqft").over(w6))
    .withColumn("sqft_share_percent_rank", F.percent_rank().over(w7))
)

remaining_keys = (
    with_windows
    .select("city", "id")
    .intersectAll(
        df_stores
        .filter(F.col("region").isin(["South", "East", "West"]))
        .select("city", "id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("cumulative_sqft_tier", F.when(F.col("cumulative_sqft") >= 76690, "HIGH").otherwise("LOW"))
    .cube("city", "cumulative_sqft_tier")
    .agg(
        F.percentile_approx("sqft_share_percent_rank", 0.9).alias("sqft_share_percent_rank_p90"),
        F.count_if(F.col("city") == "New York").alias("new_york_events"),
        F.percentile_approx("cumulative_sqft", 0.9).alias("cumulative_sqft_p90"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_rows"),
        F.percentile_approx("cumulative_sqft", 0.95).alias("cumulative_sqft_p95"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("new_york_events") < 1000)
)
