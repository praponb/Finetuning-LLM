from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("discount_pct").desc())

ranked = (
    df_promotions
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .cube(
        F.when(F.col("discount_pct") >= 0.35, "LARGE").otherwise("SMALL").alias("discount_pct_tier"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("discount_pct_per_store"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("discount_pct_per_store_2"),
    )
    .orderBy(F.col("discount_pct_per_store_2").desc())
)
