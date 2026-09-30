from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("element")
w_running_2 = Window.partitionBy("currency")
w3 = Window.partitionBy("currency", "plan")
w4 = Window.partitionBy("currency", "plan").orderBy(F.col("element_percent_rank_2").asc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_prices").alias("element"))
)

scored = (
    lines
    .select(
        "*",
        F.percent_rank().over(w_running).alias("element_percent_rank"),
        F.percent_rank().over(w_running).alias("element_percent_rank_2"),
        F.stddev_samp("element").over(w_running_2).alias("currency_stddev_samp_element"),
    )
)

scored_price_tiers = (
    scored
    .withColumn("currency_sum_element_percent_rank", F.sum("element_percent_rank").over(w3))
)

scored_price_tiers_2 = (
    scored_price_tiers
    .withColumn("element_percent_rank_2_cume_dist", F.cume_dist().over(w4))
    .withColumn("currency_sum_element_percent_rank_2", F.sum("element_percent_rank").over(w3))
)

result = (
    scored_price_tiers_2
    .select(
        "plan",
        "element",
        "element_percent_rank_2",
        "element_percent_rank",
        "element_percent_rank_2_cume_dist",
    )
    .unpivot(
        ids=["plan"],
        values=["element", "element_percent_rank_2", "element_percent_rank", "element_percent_rank_2_cume_dist"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
