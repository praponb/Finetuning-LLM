from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("currency_min_element").desc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("element"),
    )
)

filtered = (
    lines
    .filter((F.col("effective_from") >= F.to_date(F.lit("2024-04-17"))) & (F.col("currency") == "USD"))
    .select(
        "id",
        "effective_from",
        "currency",
        "element",
        F.trim("currency").alias("clean_currency"),
    )
)

base = (
    filtered
    .filter((F.col("effective_from") > F.lit("2024-06-20").cast("date")) & (F.col("currency") == "USD"))
    .select(
        "id",
        "effective_from",
        "currency",
        "element",
        F.dayofmonth("effective_from").alias("effective_from_day"),
    )
)

enriched = (
    base
    .select(
        "id",
        "currency",
        "element",
        "effective_from_day",
        F.min("element").over(w_ordered).alias("currency_min_element"),
        F.max("effective_from_day").over(w_ordered).alias("currency_max_effective_from_day"),
    )
)

metrics = (
    enriched
    .filter(F.col("effective_from_day") > 65)
    .withColumn("currency_min_element_dense_rank", F.dense_rank().over(w2))
)

result = (
    metrics
    .select(
        "currency",
        "element",
        "currency_min_element",
        "currency_max_effective_from_day",
    )
)
