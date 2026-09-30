from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("element")
w2 = Window.orderBy(F.col("element").desc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

enriched = (
    exploded
    .filter(F.col("currency").isin(["THB", "EUR"]))
    .withColumn("element_cume_dist", F.cume_dist().over(w))
    .withColumn("element_rank", F.rank().over(w2))
    .withColumn("element_percent_rank", F.percent_rank().over(w2))
)

valid = (
    enriched
    .filter(F.col("effective_from") > F.lit("2024-04-24").cast("date"))
    .select(
        "id",
        "effective_from",
        "element",
        "element_cume_dist",
        "element_rank",
        "element_percent_rank",
        F.when(F.col("element") >= 80, "High").when(F.col("element") >= 20, "Medium").otherwise("Low").alias("element_tier"),
    )
)

base = (
    valid
    .filter(F.col("element").between(45, 61))
    .select("*", F.col("element_rank").cast("double").alias("element_rank_str"))
)

result = (
    base
    .select("id", "effective_from", "element", "element_rank", "element_percent_rank")
    .orderBy(F.col("id").asc_nulls_last())
)
