from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("sale_date").asc_nulls_last())
w2 = Window.partitionBy("store_id").orderBy("revenue_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("revenue_adjusted", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("store_id").orderBy("revenue_adjusted", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("sale_date", "id")
w7 = Window.partitionBy("store_id").orderBy(F.col("revenue").desc_nulls_last(), "id")

filtered = (
    df_store_sales
    .filter(F.col("sale_date") >= F.to_date(F.lit("2023-09-23")))
    .withColumn("revenue_adjusted", F.round((F.col("revenue") * 1.15), 2))
)

deduped = (
    filtered
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    deduped
    .select(
        "*",
        F.sum("revenue").over(w2).alias("cumulative_revenue"),
        F.avg("units").over(w3).alias("units_avg_2_prior"),
        F.avg("revenue_adjusted").over(w4).alias("revenue_adjusted_avg_5"),
        F.sum("revenue_adjusted").over(w5).alias("cumulative_revenue_adjusted"),
        F.lag("revenue_adjusted", 2).over(w6).alias("prev_revenue_adjusted"),
    )
)

with_dims = (
    scored
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.opened_date"))
)

enriched = (
    with_dims
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.opened_date").alias("store_opened_date"), F.col("b.sqft"))
)

ranked = (
    enriched
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .groupBy("store_id")
    .agg(
        F.count("*").alias("cnt"),
        F.max("revenue_adjusted_avg_5").alias("max_revenue_adjusted_avg_5"),
        F.count("*").alias("event_count"),
        F.max("revenue").alias("max_revenue"),
        F.round(F.avg("prev_revenue_adjusted"), 2).alias("avg_prev_revenue_adjusted"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
)
