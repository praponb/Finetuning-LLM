from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("val").desc())
w2 = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_last())
w3 = Window.partitionBy("plan").orderBy(F.col("val").asc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("val"))
)

scored = (
    flattened
    .select(
        "*",
        F.rank().over(w).alias("val_rank"),
        F.cume_dist().over(w2).alias("val_cume_dist"),
        F.percent_rank().over(w3).alias("val_percent_rank"),
    )
)

base = (
    scored
    .filter(F.col("val_cume_dist") >= 0.31)
    .select(
        "id",
        "plan",
        "currency",
        "val_rank",
        "val_cume_dist",
        F.round((F.col("val_cume_dist") * 100), 2).alias("val_cume_dist_adjusted"),
    )
)

result = (
    base
    .groupBy("plan")
    .pivot("currency", ["THB", "EUR"])
    .agg(F.sum("val_cume_dist_adjusted").alias("amount"))
)
