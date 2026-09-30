from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "city").orderBy(F.col("sqft").desc())
w2 = Window.orderBy("latest_rank", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("city").orderBy("revenue", "id")

filtered = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-09-20").cast("date"))
    .select(
        "id",
        "store_id",
        "revenue",
        F.when(F.col("revenue") >= 4000, "High").when(F.col("revenue") >= 1100, "Medium").otherwise("Low").alias("revenue_band"),
    )
)

joined = (
    filtered
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.sqft"), F.col("b.city"))
)

joined_store_sales = (
    joined
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "inner")
    .select("a.*", F.col("s.store_name"), F.col("s.sqft").alias("store_sqft"))
)

latest = (
    joined_store_sales
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    latest
    .select(
        "id",
        "revenue",
        "sqft",
        "latest_rank",
        F.avg("store_sqft").over(w2).alias("store_sqft_avg_3_prior"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

stats = (
    with_windows
    .groupBy(F.when(F.col("sqft") >= 15347, "HIGH").otherwise("LOW").alias("sqft_tier"))
    .agg(
        F.percentile_approx("event_seq", 0.9).alias("event_seq_p90"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
)

result = stats.select("sqft_tier", "event_seq_p90", "avg_revenue")
