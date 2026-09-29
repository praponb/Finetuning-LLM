from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_first())
w2 = Window.orderBy(F.col("tier_value").asc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

result = (
    lines
    .filter(F.col("plan") == "pro")
    .withColumn("tier_value_rank", F.rank().over(w))
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w2))
)
