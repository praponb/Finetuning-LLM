from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating").asc_nulls_first(), "id")
w2 = Window.partitionBy("country").orderBy(F.col("rating_rounded").desc_nulls_first())
w3 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy("rating_rounded", "id")

filtered = (
    df_suppliers
    .filter((~F.col("city").eqNullSafe(F.lit("Singapore"))) & F.col("city").isin(["Berlin", "Chiang Mai"]))
    .select("*", F.round("rating", 1).alias("rating_rounded"))
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

cleaned = (
    latest
    .filter(F.col("rating") < 4.1)
    .select("*", F.upper(F.trim("supplier_name")).alias("supplier_name_normalized"))
)

metrics = (
    cleaned
    .select(
        "*",
        F.percent_rank().over(w2).alias("rating_rounded_percent_rank"),
        F.first("city", ignorenulls=True).over(w3).alias("first_known_city"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "rating_rounded",
        "row_num",
        "supplier_name_normalized",
        "rating_rounded_percent_rank",
        "first_known_city",
        F.lag("rating").over(w4).alias("prev_rating"),
        F.lead("rating").over(w4).alias("next_rating"),
        F.lag("rating_rounded").over(w4).alias("prev_rating_rounded"),
        F.lead("rating_rounded").over(w4).alias("next_rating_rounded"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

daily = (
    scored
    .rollup("country", "city")
    .agg(
        F.round(F.sum("prev_rating_rounded") / F.nullif(F.count(F.when(F.col("country") == "SG", 1)), F.lit(0)), 2).alias("prev_rating_rounded_per_sg"),
        F.round(F.avg("rating_rounded_percent_rank"), 2).alias("avg_rating_rounded_percent_rank"),
        F.round(F.sum("rating_rounded_percent_rank") / F.nullif(F.count(F.when(F.col("country") == "SG", 1)), F.lit(0)), 2).alias("rating_rounded_percent_rank_per_sg"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("next_rating_rounded")).otherwise(0)).alias("berlin_next_rating_rounded"),
        F.round(F.sum("prev_rating") / F.nullif(F.count(F.when(F.col("country") == "SG", 1)), F.lit(0)), 2).alias("prev_rating_per_sg"),
        F.grouping("country").alias("grouping_id"),
    )
    .filter(F.col("avg_rating_rounded_percent_rank") > 0)
)

result = (
    daily
    .select(
        "country",
        "avg_rating_rounded_percent_rank",
        "rating_rounded_percent_rank_per_sg",
        "berlin_next_rating_rounded",
        "prev_rating_per_sg",
        "grouping_id",
    )
    .orderBy(
        "rating_rounded_percent_rank_per_sg",
        F.col("avg_rating_rounded_percent_rank").desc(),
    )
)
