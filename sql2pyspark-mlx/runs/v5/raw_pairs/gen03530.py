from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("element")
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan", "currency").orderBy(F.col("element").desc())
w4 = Window.partitionBy("currency").orderBy(F.col("element").desc())
w5 = Window.orderBy(F.col("element_dense_rank_2").asc_nulls_first())
w6 = Window.orderBy(F.col("element_rank").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("element"),
    )
)

valid = (
    flattened
    .filter(F.col("element") > 56)
    .withColumn("effective_from_minus_27d", F.date_sub("effective_from", 27))
    .withColumn("plan_lower", F.lower("plan"))
)

scored = (
    valid
    .select(
        "id",
        "effective_from",
        "element",
        "effective_from_minus_27d",
        "plan_lower",
        F.dense_rank().over(w).alias("element_dense_rank"),
        F.sum("element").over(w2).alias("plan_sum_element"),
        F.rank().over(w).alias("element_rank"),
        F.dense_rank().over(w3).alias("element_dense_rank_2"),
        F.dense_rank().over(w4).alias("element_dense_rank_3"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.percent_rank().over(w5).alias("element_dense_rank_2_percent_rank"),
        F.dense_rank().over(w6).alias("element_rank_dense_rank"),
    )
)

result = (
    with_windows
    .groupBy(
        F.date_trunc("YEAR", "effective_from_minus_27d").alias("year_start"),
        F.when(F.col("element_dense_rank_3") >= 7, "PREMIUM").otherwise("STANDARD").alias("element_dense_rank_3_tier"),
    )
    .agg(
        F.percentile_approx("element_dense_rank_2", [0.25, 0.5, 0.75]).alias("element_dense_rank_2_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("element_dense_rank_2").alias("total_element_dense_rank_2"),
    )
    .filter(F.col("total_element_dense_rank_2") > 0)
)
