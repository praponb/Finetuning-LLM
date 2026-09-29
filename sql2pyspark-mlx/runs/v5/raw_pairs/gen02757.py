from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_last())
w2 = Window.orderBy("rating", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_first())
w4 = Window.partitionBy("country").orderBy("rating", "id")

base = (
    df_suppliers
    .filter(F.col("supplier_name").like("Pa%"))
    .select("id", "country", "rating", F.round("rating", 1).alias("rating_rounded"))
)

eligible = (
    base
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.cost") <= 480), "left_anti")
)

with_windows = (
    eligible
    .select(
        "id",
        "rating",
        "rating_rounded",
        F.cume_dist().over(w).alias("rating_cume_dist"),
        F.avg("rating").over(w2).alias("rating_avg_5"),
        F.cume_dist().over(w3).alias("rating_cume_dist_2"),
        F.lead("rating_rounded", 1).over(w4).alias("next_rating_rounded"),
    )
)

matched_keys = (
    with_windows
    .select("id")
    .intersectAll(
        df_suppliers
        .filter(F.col("rating") >= 2.1)
        .select("id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("next_rating_rounded") < 28)
    .withColumn("least_rating_rating_rounded", F.least("rating", "rating_rounded"))
    .withColumn("rating_cume_dist_whole", F.ceil("rating_cume_dist"))
)

cleaned = (
    valid
    .filter(F.col("rating_cume_dist_whole") < 73)
    .select(
        "*",
        (F.col("rating_avg_5") - F.col("rating_rounded")).alias("rating_avg_5_minus_rating_rounded"),
        (F.col("rating_cume_dist_2") * F.col("least_rating_rating_rounded")).alias("rating_cume_dist_2_times_least_rating_rating_rounded"),
    )
)

result = (
    cleaned
    .withColumn("least_rating_rating_rounded_tier", F.when(F.col("least_rating_rating_rounded") >= 64, "PREMIUM").otherwise("STANDARD"))
    .rollup("least_rating_rating_rounded_tier")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("rating_cume_dist"), 2).alias("avg_rating_cume_dist"),
        F.grouping("least_rating_rating_rounded_tier").alias("grouping_id"),
    )
)
