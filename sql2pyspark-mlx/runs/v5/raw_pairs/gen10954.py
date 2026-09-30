from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city")
w2 = Window.partitionBy("country").orderBy(F.col("rating").asc_nulls_last())
w_recent = Window.partitionBy("city", "country")
w_grp = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.concat("city", F.lit(", "), "supplier_name").alias("city_supplier_name"),
    )
)

enriched = (
    cleaned
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w)), 4))
    .withColumn("rating_percent_rank", F.percent_rank().over(w2))
    .withColumn("city_max_rating", F.max("rating").over(w_recent))
    .withColumn("last_known_rating", F.last("rating", ignorenulls=True).over(w_grp))
)

remaining_keys = (
    enriched
    .select("id", "city")
    .intersectAll(
        df_suppliers
        .filter(F.col("city") == "Berlin")
        .select("id", "city")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.length(F.col("c.product_name")) >= 9), "left_semi")
)

result = (
    not_flagged
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", F.col("last_known_rating"))).alias("singapore_total_last_known_rating"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count_if(F.col("country") == "JP").alias("jp_events"),
        F.count_if(F.col("city") == "London").alias("london_events"),
        F.percentile_approx("rating_share", [0.25, 0.5, 0.75]).alias("rating_share_quartiles"),
    )
)
