from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").asc_nulls_last())
w2 = Window.partitionBy("plan")
w3 = Window.orderBy(F.col("val").desc_nulls_last())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

result = (
    flattened
    .withColumn("val_rank", F.rank().over(w))
    .withColumn("plan_avg_val", F.avg("val").over(w2))
    .withColumn("plan_min_val", F.min("val").over(w2))
    .withColumn("val_rank_2", F.rank().over(w3))
)
