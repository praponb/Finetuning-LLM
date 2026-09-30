from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("element").asc_nulls_first())
w3 = Window.partitionBy("currency").orderBy(F.col("element").asc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("element"),
    )
)

kept_keys = (
    exploded
    .select("id", "currency")
    .exceptAll(
        df_price_tiers
        .filter(F.col("effective_from") <= "2024-04-01")
        .select("id", "currency")
    )
)

verified = (
    exploded
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("currency").isin(["THB", "USD", "EUR"]))
    .withColumn("currency_group", F.when(F.col("currency").isin(["USD", "THB"]), "Group A").otherwise("Other"))
)

scored = (
    cleaned
    .select(
        "id",
        "element",
        "currency_group",
        F.count("element").over(w).alias("currency_count_element"),
        F.cume_dist().over(w2).alias("element_cume_dist"),
        F.dense_rank().over(w2).alias("element_dense_rank"),
        F.cume_dist().over(w3).alias("element_cume_dist_2"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("element_cume_dist_2") >= 0.73, "LARGE").otherwise("SMALL").alias("element_cume_dist_2_tier"),
    )
    .agg(
        F.percentile_approx("element_cume_dist_2", 0.95).alias("element_cume_dist_2_p95"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.round(F.avg("element"), 2).alias("avg_element"),
        F.sum("element_cume_dist").alias("total_element_cume_dist"),
        F.countDistinct("id").alias("distinct_ids_3"),
    )
    .filter(F.col("element_cume_dist_2_p95") > 0)
    .orderBy("element_cume_dist_2_p95")
)
