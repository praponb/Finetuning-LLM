from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w_recent = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("store_id").orderBy("sale_date")
w6 = Window.partitionBy("product_id").orderBy("sale_date", "id")
w7 = Window.partitionBy("store_id").orderBy(F.col("revenue").desc())
w8 = Window.partitionBy("store_id", "product_id").orderBy(F.col("units_avg_5").asc_nulls_last(), "id")

with_windows = (
    df_store_sales
    .select(
        "*",
        F.round((F.col("units") / F.sum("units").over(w)), 4).alias("units_share"),
        F.sum("units").over(w_recent).alias("cumulative_units"),
        F.avg("units").over(w3).alias("units_avg_5"),
        F.avg("revenue").over(w4).alias("revenue_avg_6_prior"),
    )
)

ranked = (
    with_windows
    .withColumn("latest_rank", F.rank().over(w5))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    ranked
    .filter(F.col("units_share").isNotNull())
    .withColumn("prev_cumulative_units", F.lag("cumulative_units").over(w6))
    .withColumn("next_units_avg_5", F.lead("units_avg_5").over(w6))
    .withColumn("event_seq", F.row_number().over(w6))
    .withColumn("revenue_dense_rank", F.dense_rank().over(w7))
)

ranked_store_sales = (
    enriched
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
)

joined = (
    ranked_store_sales
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.region"), F.col("p.city"))
)

result = (
    joined
    .groupBy("region", F.date_trunc("QUARTER", "sale_date").alias("quarter_start"))
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("next_units_avg_5", [0.25, 0.5, 0.75]).alias("next_units_avg_5_quartiles"),
        F.round(F.avg("revenue_avg_6_prior"), 2).alias("avg_revenue_avg_6_prior"),
        F.round(F.avg("units_share"), 2).alias("avg_units_share"),
        F.max(F.when(F.col("city") == "New York", F.col("prev_cumulative_units"))).alias("new_york_max_prev_cumulative_units"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
    .orderBy(F.col("region").desc_nulls_first())
)
