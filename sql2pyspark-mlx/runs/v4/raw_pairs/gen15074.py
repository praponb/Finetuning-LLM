from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("ends_on").desc(), "id")

latest = (
    df_promotions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .groupBy(
        F.when(F.col("discount_pct") >= 0.33, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
    )
    .agg(
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.count_if(F.col("promo_code") == "SPRING10").alias("spring10_events"),
        F.min("discount_pct").alias("min_discount_pct"),
    )
    .orderBy("avg_discount_pct")
)
