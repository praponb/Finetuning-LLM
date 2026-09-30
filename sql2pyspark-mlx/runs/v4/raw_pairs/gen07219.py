from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_first())
w_running = Window.orderBy(F.col("val").asc_nulls_first())
w3 = Window.partitionBy("currency").orderBy(F.col("val_percent_rank").desc())
w4 = Window.partitionBy("currency").orderBy(F.col("val_percent_rank").desc_nulls_first())

exploded = df_price_tiers.select("id", "currency", F.explode("tier_prices").alias("val"))

scored = (
    exploded
    .withColumn("val_rank", F.rank().over(w_part))
    .withColumn("val_percent_rank", F.percent_rank().over(w_running))
    .withColumn("val_dense_rank", F.dense_rank().over(w_part))
)

with_windows = (
    scored
    .select(
        "*",
        F.dense_rank().over(w_part).alias("val_dense_rank_2"),
        F.dense_rank().over(w3).alias("val_percent_rank_dense_rank"),
        F.cume_dist().over(w4).alias("val_percent_rank_cume_dist"),
    )
)

result = (
    with_windows
    .select("currency", "val_percent_rank_cume_dist", "val")
    .unpivot(["currency"], ["val_percent_rank_cume_dist", "val"], "kpi", "value")
    .filter(F.col("value").isNotNull())
)
