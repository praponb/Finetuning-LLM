from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("supplier_name_length").desc_nulls_last())
w_ordered = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country", "city").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("city", "country").orderBy("running_rating", "id")

base = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("country").isin(["JP", "UK"]))
    .withColumn("supplier_name_length", F.length("supplier_name"))
)

latest = (
    base
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    latest
    .filter(F.col("rating") <= 3.9)
    .select(
        "*",
        F.sum("rating").over(w_ordered).alias("running_rating"),
        F.sum("rating").over(w3).alias("cumulative_rating"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.lead("supplier_name_length").over(w_recent).alias("next_supplier_name_length"),
        F.lead("cumulative_rating").over(w_recent).alias("next_cumulative_rating"),
        F.lag("cumulative_rating").over(w_recent).alias("prev_cumulative_rating"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)

result = (
    with_windows
    .withColumn("rating_tier", F.when(F.col("rating") >= 3.5, "LARGE").otherwise("SMALL"))
    .groupingSets([["country", "rating_tier"], ["country"]], "country", "rating_tier")
    .agg(
        F.percentile_approx("running_rating", [0.25, 0.5, 0.75]).alias("running_rating_quartiles"),
        F.countDistinct("country").alias("distinct_countries"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.countDistinct("city").alias("distinct_cities"),
        F.round(F.sum("prev_cumulative_rating") / F.nullif(F.count(F.when(F.col("country") == "TH", 1)), F.lit(0)), 2).alias("prev_cumulative_rating_per_th"),
    )
    .orderBy(F.col("distinct_cities").desc_nulls_first())
)
