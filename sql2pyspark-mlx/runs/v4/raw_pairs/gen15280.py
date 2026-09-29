from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc_nulls_first())
w2 = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_first())
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy("currency")
w5 = Window.partitionBy("plan").orderBy(F.col("plan_max_element").desc())
w_hist = Window.partitionBy("plan", "currency").orderBy(F.col("element_cume_dist").desc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_prices").alias("element"))
)

valid = (
    lines
    .filter(F.col("plan") == "starter")
    .withColumn("plan_title", F.initcap(F.trim("plan")))
)

with_windows = (
    valid
    .select(
        "*",
        F.cume_dist().over(w).alias("element_cume_dist"),
        F.dense_rank().over(w2).alias("element_dense_rank"),
        F.max("element").over(w3).alias("plan_max_element"),
        F.sum("element").over(w4).alias("currency_sum_element"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.percent_rank().over(w5).alias("plan_max_element_percent_rank"),
        F.cume_dist().over(w_hist).alias("element_cume_dist_cume_dist"),
    )
)

result = (
    scored
    .select(
        "plan",
        "element",
        "element_cume_dist",
        "element_dense_rank",
        "element_cume_dist_cume_dist",
    )
)
