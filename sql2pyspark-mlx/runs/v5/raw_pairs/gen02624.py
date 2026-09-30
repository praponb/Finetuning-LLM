from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_last(), "id")
w2 = Window.partitionBy("city")
w3 = Window.orderBy(F.col("rating").desc())
w4 = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_first())

latest = (
    df_suppliers
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "city", "rating")
)

remaining_keys = (
    latest
    .select("id", "city")
    .intersect(
        df_suppliers
        .filter(F.col("supplier_name").like("G%"))
        .select("id", "city")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

kept = (
    reconciled
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.brand").isin(["Globex", "Initech", "Acme"])), "left_anti")
)

eligible = (
    kept
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.size(F.col("c.tags")) > 2), "left_anti")
)

kept_suppliers = (
    eligible
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.price") > 1070), "left_anti")
)

not_flagged = (
    kept_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.length(F.col("ch.product_name")) > 12), "left_semi")
)

enriched = (
    not_flagged
    .select(
        "*",
        F.sum("rating").over(w2).alias("city_sum_rating"),
        F.round((F.col("rating") / F.sum("rating").over(w2)), 4).alias("rating_share"),
        F.dense_rank().over(w3).alias("rating_dense_rank"),
        F.rank().over(w4).alias("rating_rank"),
        F.stddev_samp("rating").over(w2).alias("city_stddev_samp_rating"),
    )
)

result = (
    enriched
    .groupBy("city")
    .agg(
        F.count("*").alias("event_count"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count_if(F.col("city") == "London").alias("london_events"),
        F.round(F.avg("rating_rank"), 2).alias("avg_rating_rank"),
        F.round(F.sum("rating_share") / F.nullif(F.count(F.when(F.col("city") == "Bangkok", 1)), F.lit(0)), 2).alias("rating_share_per_bangkok"),
        F.round(F.sum("rating_rank") / F.nullif(F.count(F.when(F.col("city") == "Berlin", 1)), F.lit(0)), 2).alias("rating_rank_per_berlin"),
    )
)
