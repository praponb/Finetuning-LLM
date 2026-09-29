from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc_nulls_first())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan").orderBy("element")
w4 = Window.orderBy(F.col("element_rank").desc_nulls_last())
w5 = Window.partitionBy("plan").orderBy(F.col("element_cume_dist").desc_nulls_last())
w6 = Window.partitionBy("plan_count_element_tier").orderBy(F.col("distinct_ids").desc())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_prices").alias("element"),
    )
)

enriched = (
    lines
    .withColumn("element_rank", F.rank().over(w))
    .withColumn("plan_count_element", F.count("element").over(w2))
    .withColumn("element_cume_dist", F.cume_dist().over(w3))
)

valid = (
    enriched
    .filter(F.year("effective_from") >= 2024)
    .select("*", F.col("element_rank").cast("string").alias("element_rank_num"))
)

scored = (
    valid
    .select(
        "id",
        "plan",
        "element",
        "plan_count_element",
        "element_cume_dist",
        F.cume_dist().over(w4).alias("element_rank_cume_dist"),
        F.percent_rank().over(w5).alias("element_cume_dist_percent_rank"),
    )
)

base = (
    scored
    .filter(F.col("element_cume_dist").between(0.23, 0.29))
    .select(
        "id",
        "element",
        "plan_count_element",
        "element_cume_dist_percent_rank",
        (F.col("element_rank_cume_dist") - F.col("element")).alias("element_rank_cume_dist_minus_element"),
        F.col("element").cast("int").alias("element_int"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("plan_count_element") >= 14, "HIGH").otherwise("LOW").alias("plan_count_element_tier"),
        F.when(F.col("element_rank_cume_dist_minus_element") >= 72, "LARGE").otherwise("SMALL").alias("element_rank_cume_dist_minus_element_tier"),
        F.when(F.col("element_cume_dist_percent_rank") >= 0.71, "PREMIUM").otherwise("STANDARD").alias("element_cume_dist_percent_rank_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("plan_count_element", [0.25, 0.5, 0.75]).alias("plan_count_element_quartiles"),
        F.count("*").alias("row_count"),
    )
    .withColumn("distinct_ids_rank", F.rank().over(w6))
    .orderBy("plan_count_element_tier")
)
