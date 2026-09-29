from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id")
w2 = Window.orderBy("sqft")
w_recent = Window.partitionBy("region", "city").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy(F.col("next_sqft").desc())

with_windows = (
    df_stores
    .withColumn("next_sqft", F.lead("sqft").over(w))
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("sqft_cume_dist", F.cume_dist().over(w2))
)

with_windows_stores = (
    with_windows
    .select(
        "id",
        "city",
        "next_sqft",
        "seq_no",
        "sqft_cume_dist",
        F.max("sqft_cume_dist").over(w_recent).alias("running_max_sqft_cume_dist"),
        F.sum("sqft_cume_dist").over(w_recent).alias("running_sum_sqft_cume_dist"),
        F.count("sqft_cume_dist").over(w_recent).alias("running_count_sqft_cume_dist"),
    )
)

deduped = (
    with_windows_stores
    .withColumn("rn", F.rank().over(w4))
    .filter(F.col("rn") == 1)
)

remaining_keys = (
    deduped
    .select("id", "city")
    .exceptAll(
        df_stores
        .filter(F.col("region").isin(["South", "East"]))
        .select("id", "city")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

stats = (
    reconciled
    .groupBy(
        F.when(F.col("running_max_sqft_cume_dist") >= 0.9, "LARGE").otherwise("SMALL").alias("running_max_sqft_cume_dist_tier"),
        "city",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sort_array(F.collect_set("city")).alias("city_list_2"),
    )
)

result = (
    stats
    .select(
        "running_max_sqft_cume_dist_tier",
        "city",
        "distinct_ids",
        "city_list",
        "city_list_2",
    )
    .orderBy("running_max_sqft_cume_dist_tier")
)
