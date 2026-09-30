from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("element").desc_nulls_last())
w3 = Window.partitionBy("plan", "currency").orderBy(F.col("element").desc())
w_part = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_last())
w5 = Window.partitionBy("plan").orderBy(F.col("element").desc())
w6 = Window.partitionBy("currency", "plan")
w7 = Window.partitionBy("plan").orderBy(F.col("currency_avg_element_2").desc_nulls_first())
w8 = Window.partitionBy("plan").orderBy(F.col("element_percent_rank").asc_nulls_last())

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("discounts").alias("element"))
)

base = (
    flattened
    .filter(F.col("plan") == "enterprise")
    .select("*", F.upper("currency").alias("currency_upper"))
)

enriched = (
    base
    .select(
        "id",
        "plan",
        "currency",
        "element",
        F.avg("element").over(w_ordered).alias("currency_avg_element"),
        F.percent_rank().over(w2).alias("element_percent_rank"),
        F.avg("element").over(w_ordered).alias("currency_avg_element_2"),
        F.percent_rank().over(w3).alias("element_percent_rank_2"),
        F.rank().over(w_part).alias("element_rank"),
    )
)

base_price_tiers = (
    enriched
    .filter(F.col("element_percent_rank").between(0.25, 0.34))
    .withColumn("currency_avg_element_2_plus_currency_avg_element", (F.col("currency_avg_element_2") + F.col("currency_avg_element")))
)

scored = (
    base_price_tiers
    .filter(F.col("element") <= 80)
    .select(
        "plan",
        "element",
        "currency_avg_element",
        "element_percent_rank",
        "element_percent_rank_2",
        F.percent_rank().over(w5).alias("element_percent_rank_3"),
        F.sum("element_rank").over(w6).alias("currency_sum_element_rank"),
        F.rank().over(w7).alias("currency_avg_element_2_rank"),
        F.stddev_samp("currency_avg_element_2").over(w_ordered).alias("currency_stddev_samp_currency_avg_element_2"),
        F.dense_rank().over(w8).alias("element_percent_rank_dense_rank"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("currency_avg_element") >= 71, "PREMIUM").otherwise("STANDARD").alias("currency_avg_element_tier"),
    )
    .agg(
        F.round(F.sum("currency_sum_element_rank") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("currency_sum_element_rank_per_basic"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("element_percent_rank_2")).otherwise(0)).alias("enterprise_element_percent_rank_2"),
        F.round(F.avg("element_percent_rank_3"), 2).alias("avg_element_percent_rank_3"),
        F.sum(F.when(F.col("plan") == "basic", F.col("element_percent_rank"))).alias("basic_total_element_percent_rank"),
        F.sum(F.when(F.col("plan") == "starter", F.col("element_percent_rank_2")).otherwise(0)).alias("starter_element_percent_rank_2"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
)
