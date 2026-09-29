from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc(), "id")

latest = (
    df_promotions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    latest
    .rollup(
        F.when(F.col("discount_pct") >= 0.27, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
        F.count(F.when(F.col("promo_code") == "WELCOME5", 1)).alias("welcome5_count"),
        F.max("discount_pct").alias("max_discount_pct"),
        F.avg("discount_pct").alias("avg_discount_pct"),
    )
)
