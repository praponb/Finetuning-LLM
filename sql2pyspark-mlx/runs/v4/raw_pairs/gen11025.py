from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft_rounded")
w2 = Window.orderBy(F.col("opened_date").desc(), "id")
w3 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_first())
w5 = Window.partitionBy("region")
w6 = Window.partitionBy("city", "region").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("region").orderBy("sqft_rounded_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_stores
    .filter(F.col("opened_date") > F.to_date(F.lit("2023-12-27")))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.col("sqft").cast("string").alias("sqft_num"),
        F.round("sqft", 0).alias("sqft_rounded"),
    )
)

scored = (
    base
    .select(
        "*",
        F.cume_dist().over(w).alias("sqft_rounded_cume_dist"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

scored_stores = (
    scored
    .select(
        "*",
        F.sum("seq_no").over(w3).alias("running_seq_no"),
        F.cume_dist().over(w4).alias("sqft_cume_dist"),
        F.round((F.col("seq_no") / F.sum("seq_no").over(w5)), 4).alias("seq_no_share"),
        F.sum("sqft").over(w6).alias("cumulative_sqft"),
    )
)

with_windows = (
    scored_stores
    .select(
        "*",
        F.first("running_seq_no", ignorenulls=True).over(w7).alias("first_known_running_seq_no"),
        F.avg("seq_no_share").over(w8).alias("seq_no_share_avg_6"),
    )
)

totals = (
    with_windows
    .withColumn("sqft_rounded_tier", F.when(F.col("sqft_rounded") >= 89, "LARGE").otherwise("SMALL"))
    .cube("sqft_rounded_tier")
    .agg(
        F.round(F.sum("seq_no") / F.nullif(F.count(F.when(F.col("region") == "West", 1)), F.lit(0)), 2).alias("seq_no_per_west"),
        F.sum(F.when(F.col("region") == "North", F.col("cumulative_sqft")).otherwise(0)).alias("north_cumulative_sqft"),
        F.sum(F.when(F.col("region") == "East", F.col("cumulative_sqft")).otherwise(0)).alias("east_cumulative_sqft"),
        F.sum(F.when(F.col("city") == "Singapore", F.col("sqft_cume_dist"))).alias("singapore_total_sqft_cume_dist"),
        F.count("*").alias("cnt"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    totals
    .select("seq_no_per_west", "gid")
    .orderBy(F.col("seq_no_per_west").desc_nulls_first())
)
