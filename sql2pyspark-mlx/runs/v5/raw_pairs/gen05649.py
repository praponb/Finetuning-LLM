from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("city").orderBy(F.col("running_sum_rating_whole").desc_nulls_last())

enriched = (
    df_products
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "e.id",
        "e.launch_date",
        "e.supplier_id",
        F.col("d.rating"),
        F.col("d.city"),
    )
)

enriched_products = (
    enriched
    .select(
        "id",
        "rating",
        "city",
        F.min("rating").over(w).alias("running_min_rating"),
        F.max("rating").over(w).alias("running_max_rating"),
        F.sum("rating").over(w).alias("running_sum_rating"),
    )
)

filtered = (
    enriched_products
    .filter(F.col("city").isin(["Singapore", "Chiang Mai"]))
    .select(
        "id",
        "rating",
        "city",
        "running_max_rating",
        F.when(F.col("running_min_rating") >= 4.2, "High").when(F.col("running_min_rating") >= 1.5, "Medium").otherwise("Low").alias("running_min_rating_tier"),
        F.floor("running_sum_rating").alias("running_sum_rating_whole"),
        F.coalesce("city", F.lit("DIRECT")).alias("resolved_city"),
    )
)

remaining_keys = (
    filtered
    .select("id")
    .subtract(
        df_products
        .filter(F.col("brand") != "Stark")
        .select("id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "*",
        F.round((F.col("running_max_rating") / F.sum("running_max_rating").over(w2)), 4).alias("running_max_rating_share"),
        F.percent_rank().over(w3).alias("running_sum_rating_whole_percent_rank"),
    )
)

result = (
    with_windows
    .groupBy("city")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.percentile_approx("running_max_rating", 0.95).alias("running_max_rating_p95"),
    )
    .orderBy("city")
)
