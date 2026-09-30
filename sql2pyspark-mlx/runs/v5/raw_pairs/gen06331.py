from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("element").desc())
w2 = Window.partitionBy("plan").orderBy("element")
w3 = Window.orderBy(F.col("element").desc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("element"),
    )
)

valid = (
    exploded
    .filter(F.col("effective_from") >= F.lit("2024-01-26").cast("date"))
    .select(
        "id",
        "plan",
        "effective_from",
        "element",
        F.upper("currency").alias("currency_upper"),
    )
)

cleaned = (
    valid
    .filter(F.col("effective_from") > F.lit("2024-02-15").cast("date"))
    .select(
        "id",
        "plan",
        "effective_from",
        "element",
        F.weekofyear("effective_from").alias("effective_from_weekofyear"),
    )
)

filtered = (
    cleaned
    .filter(F.col("effective_from") > F.lit("2024-03-07").cast("date"))
    .select(
        "id",
        "plan",
        "effective_from",
        "element",
        "effective_from_weekofyear",
        F.when(F.col("plan").isin(["enterprise", "starter"]), "Tier 1").otherwise("Other").alias("plan_group"),
    )
)

result = (
    filtered
    .filter(F.year("effective_from") >= 2025)
    .withColumn("element_dense_rank", F.dense_rank().over(w))
    .withColumn("element_percent_rank", F.percent_rank().over(w2))
    .withColumn("element_dense_rank_2", F.dense_rank().over(w3))
)
