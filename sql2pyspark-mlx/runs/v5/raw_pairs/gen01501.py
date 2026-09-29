from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("element").asc_nulls_last())
w2 = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_last())
w3 = Window.partitionBy("currency").orderBy(F.col("element").desc())
w4 = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_last())
w5 = Window.partitionBy("currency")
w6 = Window.partitionBy("element_cume_dist_tier").orderBy(F.col("avg_least_element_percent_rank_currency_stddev_samp_element").desc())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("element"))
)

metrics = (
    lines
    .select(
        "id",
        "plan",
        F.rank().over(w).alias("element_rank"),
        F.percent_rank().over(w2).alias("element_percent_rank"),
        F.cume_dist().over(w3).alias("element_cume_dist"),
        F.percent_rank().over(w4).alias("element_percent_rank_2"),
        F.stddev_samp("element").over(w5).alias("currency_stddev_samp_element"),
    )
)

base = (
    metrics
    .filter(F.col("plan").isin(["starter", "enterprise"]))
    .select(
        "*",
        F.least("element_percent_rank", "currency_stddev_samp_element").alias("least_element_percent_rank_currency_stddev_samp_element"),
    )
)

valid = (
    base
    .filter(F.col("plan") == "enterprise")
    .select(
        "id",
        "plan",
        "element_rank",
        "element_cume_dist",
        "currency_stddev_samp_element",
        "least_element_percent_rank_currency_stddev_samp_element",
        F.when(F.col("plan") == "pro", 1).otherwise(0).alias("is_pro"),
        F.when(F.col("currency_stddev_samp_element") >= 87, "High").when(F.col("currency_stddev_samp_element") >= 11, "Medium").otherwise("Low").alias("currency_stddev_samp_element_level"),
    )
)

base_price_tiers = (
    valid
    .filter(F.col("element_rank") <= 3)
    .select(
        "id",
        "plan",
        "element_cume_dist",
        "currency_stddev_samp_element",
        "least_element_percent_rank_currency_stddev_samp_element",
        "is_pro",
        "currency_stddev_samp_element_level",
        (F.col("element_rank") + F.col("least_element_percent_rank_currency_stddev_samp_element")).alias("element_rank_plus_least_element_percent_rank_currency_stddev_samp_element"),
        F.trim("plan").alias("clean_plan"),
    )
)

result = (
    base_price_tiers
    .groupBy(
        F.when(F.col("element_cume_dist") >= 0.81, "HIGH").otherwise("LOW").alias("element_cume_dist_tier"),
        "plan",
    )
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("least_element_percent_rank_currency_stddev_samp_element").alias("avg_least_element_percent_rank_currency_stddev_samp_element"),
        F.round(F.avg("is_pro"), 2).alias("avg_is_pro"),
    )
    .filter(F.col("basic_count") > 1)
    .withColumn("avg_least_element_percent_rank_currency_stddev_samp_element_dense_rank", F.dense_rank().over(w6))
    .orderBy(F.col("plan").desc_nulls_last())
)
