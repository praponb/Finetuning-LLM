from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("element")
w2 = Window.orderBy(F.col("element").desc())
w3 = Window.orderBy(F.col("element_cume_dist").desc())

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_prices").alias("element"))
)

scored = (
    exploded
    .select(
        "*",
        F.dense_rank().over(w).alias("element_dense_rank"),
        F.cume_dist().over(w2).alias("element_cume_dist"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "effective_from",
        F.dense_rank().over(w3).alias("element_cume_dist_dense_rank"),
        F.rank().over(w2).alias("element_rank"),
        F.cume_dist().over(w).alias("element_cume_dist_2"),
    )
)

valid = (
    metrics
    .filter(F.col("effective_from") > F.lit("2024-05-24").cast("date"))
    .withColumn("element_cume_dist_2_filled", F.coalesce("element_cume_dist_2", F.lit(0)))
)

cleaned = (
    valid
    .filter((F.col("effective_from") > F.to_date(F.lit("2024-01-28"))) & (F.col("element_cume_dist_dense_rank") != 3))
    .select(
        "*",
        F.greatest("element_cume_dist_2", "element_rank").alias("greatest_element_cume_dist_2_element_rank"),
    )
)

result = (
    cleaned
    .groupBy(
        F.date_trunc("MONTH", "effective_from").alias("activity_month"),
        F.date_trunc("WEEK", "effective_from").alias("week_start"),
        F.when(F.col("element_rank") >= 7, "LARGE").otherwise("SMALL").alias("element_rank_tier"),
    )
    .agg(
        F.percentile_approx("element_cume_dist_2_filled", 0.95).alias("element_cume_dist_2_filled_p95"),
        F.max("element_cume_dist_2").alias("max_element_cume_dist_2"),
    )
    .orderBy(F.col("activity_month").desc())
)
