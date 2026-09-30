from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_last())
w_grp = Window.partitionBy("country").orderBy("rating", "id")
w3 = Window.partitionBy("city").orderBy(F.col("rating_bucket").desc_nulls_first())
w4 = Window.partitionBy("city").orderBy("rating_percent_rank", "id")
w5 = Window.partitionBy("country").orderBy("rating_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city").orderBy("rating", "id")
w7 = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_last())
w8 = Window.partitionBy("city", "country").orderBy(F.col("last_known_rating_percent_rank").asc_nulls_first())
w_ordered = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("city", "country").orderBy("last_known_rating_percent_rank_percent_rank", "id")
w11 = Window.partitionBy("rating_bucket_2_tier").orderBy(F.col("min_rating_bucket_cume_dist").desc())

cleaned = (
    df_suppliers
    .filter(~F.col("city").isin(["Berlin", "Tokyo", "London"]))
    .select("*", F.coalesce("city", F.lit("N/A")).alias("city_filled"))
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.stock_qty").between(124, 211)), "left_anti")
)

scored = (
    not_flagged
    .select(
        "*",
        F.percent_rank().over(w).alias("rating_percent_rank"),
        F.ntile(10).over(w_grp).alias("rating_bucket"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.cume_dist().over(w3).alias("rating_bucket_cume_dist"),
        F.row_number().over(w4).alias("event_seq"),
        F.last("rating_percent_rank", ignorenulls=True).over(w5).alias("last_known_rating_percent_rank"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.ntile(4).over(w6).alias("rating_bucket_2"),
        F.percent_rank().over(w7).alias("rating_percent_rank_2"),
        F.percent_rank().over(w8).alias("last_known_rating_percent_rank_percent_rank"),
        F.sum("rating_bucket_cume_dist").over(w_ordered).alias("cumulative_rating_bucket_cume_dist"),
    )
)

latest = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w10))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    latest
    .groupBy(
        F.when(F.col("rating_bucket_2") >= 8, "HIGH").otherwise("LOW").alias("rating_bucket_2_tier"),
        F.when(F.col("event_seq") >= 9, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
        F.when(F.col("cumulative_rating_bucket_cume_dist") >= 4, "LARGE").otherwise("SMALL").alias("cumulative_rating_bucket_cume_dist_tier"),
    )
    .agg(
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_rows"),
        F.max("rating_bucket_cume_dist").alias("max_rating_bucket_cume_dist"),
        F.min("rating_bucket_cume_dist").alias("min_rating_bucket_cume_dist"),
    )
    .withColumn("min_rating_bucket_cume_dist_rank", F.rank().over(w11))
    .orderBy(F.col("cumulative_rating_bucket_cume_dist_tier").desc())
)
