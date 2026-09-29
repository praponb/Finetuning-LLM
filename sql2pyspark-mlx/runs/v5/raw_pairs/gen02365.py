from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tier_value_tier").orderBy(F.col("distinct_ids").desc())

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("tier_value"),
    )
)

result = (
    lines
    .groupBy(
        F.when(F.col("tier_value") >= 80, "PREMIUM").otherwise("STANDARD").alias("tier_value_tier"),
        "currency",
    )
    .agg(
        F.percentile_approx("tier_value", 0.9).alias("tier_value_p90"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("currency")).alias("currency_list_2"),
    )
    .withColumn("distinct_ids_rank", F.rank().over(w))
    .orderBy(F.col("tier_value_tier").desc_nulls_first())
)
