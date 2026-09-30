from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("element").asc_nulls_first())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan", "currency").orderBy("element")

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("discounts").alias("element"))
)

with_windows = (
    flattened
    .filter(F.col("element") >= 30)
    .select(
        "*",
        F.dense_rank().over(w).alias("element_dense_rank"),
        F.avg("element").over(w2).alias("plan_avg_element"),
        F.percent_rank().over(w3).alias("element_percent_rank"),
    )
)

base = (
    with_windows
    .filter(F.col("element_dense_rank") < 5)
    .select(
        "plan",
        "element",
        "plan_avg_element",
        (F.col("plan_avg_element") + F.col("element_dense_rank")).alias("plan_avg_element_plus_element_dense_rank"),
    )
)

filtered = (
    base
    .filter(F.col("plan_avg_element").isNotNull())
    .select(
        "*",
        F.when(F.col("plan_avg_element_plus_element_dense_rank") >= 66, "High").when(F.col("plan_avg_element_plus_element_dense_rank") >= 18, "Medium").otherwise("Low").alias("plan_avg_element_plus_element_dense_rank_band"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("plan_avg_element_plus_element_dense_rank") >= 68, "LARGE").otherwise("SMALL").alias("plan_avg_element_plus_element_dense_rank_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
    )
    .orderBy("plan_avg_element_plus_element_dense_rank_tier")
)
