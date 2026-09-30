from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc_nulls_first())
w2 = Window.orderBy(F.col("element").desc())
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy("currency", "plan")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("element"),
    )
)

scored = (
    flattened
    .filter(F.col("currency") == "THB")
    .withColumn("element_rank", F.rank().over(w))
    .withColumn("element_percent_rank", F.percent_rank().over(w2))
    .withColumn("plan_min_element", F.min("element").over(w3))
    .withColumn("currency_max_element", F.max("element").over(w4))
)

filtered = (
    scored
    .filter(F.col("plan_min_element") < 53)
    .select(
        "effective_from",
        "currency",
        "element",
        "element_rank",
        "element_percent_rank",
        "plan_min_element",
        "currency_max_element",
        F.lower("plan").alias("plan_lower"),
    )
)

valid = (
    filtered
    .filter(~F.col("currency").isin(["EUR", "USD"]))
    .select(
        "currency",
        "element",
        "element_rank",
        "element_percent_rank",
        "plan_min_element",
        "currency_max_element",
        (F.col("plan_min_element") + F.col("currency_max_element")).alias("plan_min_element_plus_currency_max_element"),
        F.round("currency_max_element", 0).alias("currency_max_element_rounded"),
    )
)

result = (
    valid
    .groupBy("currency")
    .agg(
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.avg("plan_min_element_plus_currency_max_element").alias("avg_plan_min_element_plus_currency_max_element"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
    )
    .orderBy(F.col("avg_plan_min_element_plus_currency_max_element").asc_nulls_last())
)
