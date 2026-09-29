from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("element").asc_nulls_last())
w3 = Window.orderBy("element")

flattened = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("element"),
    )
)

scored = (
    flattened
    .select(
        "*",
        F.avg("element").over(w).alias("currency_avg_element"),
        F.rank().over(w2).alias("element_rank"),
        F.count("element").over(w).alias("currency_count_element"),
        F.rank().over(w3).alias("element_rank_2"),
    )
)

cleaned = (
    scored
    .filter((F.col("effective_from") > F.lit("2024-06-10").cast("date")) & (F.col("element_rank") >= 6))
    .select(
        "id",
        "effective_from",
        "currency",
        "element",
        "currency_avg_element",
        "element_rank",
        "currency_count_element",
        "element_rank_2",
        F.initcap(F.trim("currency")).alias("currency_title"),
    )
)

result = (
    cleaned
    .select("id", "currency", "currency_avg_element", "element_rank", "currency_title")
    .orderBy(F.col("id").desc())
)
