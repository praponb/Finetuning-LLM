from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "effective_from", F.explode("tier_limits").alias("element"))
)

result = (
    flattened
    .withColumn("year_start", F.date_trunc("YEAR", "effective_from"))
    .cube("year_start")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("element"), 2).alias("avg_element"),
        F.count("*").alias("row_count"),
        F.grouping_id().alias("grouping_level"),
    )
)
