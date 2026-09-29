from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(-5, -1)
w3 = Window.orderBy(F.col("sqft").asc_nulls_first())
w4 = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_last(), "id")
w5 = Window.partitionBy("region").orderBy(F.col("opened_date").desc())
w6 = Window.partitionBy("city").orderBy(F.col("sqft").asc_nulls_last())

metrics = (
    df_stores
    .select(
        "*",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 4).alias("sqft_share"),
        F.avg("sqft").over(w2).alias("sqft_avg_5_prior"),
        F.cume_dist().over(w3).alias("sqft_cume_dist"),
    )
)

deduped = (
    metrics
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked = (
    deduped
    .withColumn("row_num", F.rank().over(w5))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

matched_keys = (
    ranked
    .select("city")
    .exceptAll(
        df_stores
        .filter(F.col("sqft").between(8080, 12674))
        .select("city")
    )
)

verified = (
    ranked
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") < "2024-05-01"), "left_semi")
)

metrics_stores = (
    eligible
    .withColumn("sqft_percent_rank", F.percent_rank().over(w6))
    .withColumn("sqft_share_2", F.round((F.col("sqft") / F.sum("sqft").over(w)), 4))
)

kept_keys = (
    metrics_stores
    .select("city", "id")
    .exceptAll(
        df_stores
        .filter(F.col("sqft").between(8928, 13309))
        .select("city", "id")
    )
)

reconciled = (
    metrics_stores
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("region")
    .agg(
        F.percentile_approx("sqft_percent_rank", [0.25, 0.5, 0.75]).alias("sqft_percent_rank_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("sqft_percent_rank")).otherwise(0)).alias("berlin_sqft_percent_rank"),
    )
)
