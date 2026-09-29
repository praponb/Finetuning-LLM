from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency", "plan")
w2 = Window.partitionBy("plan").orderBy(F.col("effective_from_dayofweek").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("element"),
    )
)

base = (
    flattened
    .filter(~F.col("plan").isin(["pro", "starter"]))
    .select("*", F.dayofweek("effective_from").alias("effective_from_dayofweek"))
)

valid = (
    base
    .filter(~F.col("currency").isin(["THB", "EUR"]))
    .withColumn("currency_prefix", F.substring("currency", 1, 3))
)

enriched = (
    valid
    .withColumn("currency_sum_element", F.sum("element").over(w))
    .withColumn("effective_from_dayofweek_percent_rank", F.percent_rank().over(w2))
)

result = (
    enriched
    .groupBy("currency")
    .pivot("plan", ["pro", "starter", "enterprise"])
    .agg(F.max("effective_from_dayofweek"))
)
