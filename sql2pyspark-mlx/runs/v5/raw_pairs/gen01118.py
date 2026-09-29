from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("starts_on", "id")
w2 = Window.partitionBy("channel", "promo_code").orderBy(F.col("starts_on").desc(), "id")
w3 = Window.partitionBy("channel").orderBy("rank_in_group", "id")
w4 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_promotions
    .filter((F.col("ends_on") >= F.lit("2023-09-11").cast("date")) & F.col("channel").isNotNull())
    .withColumn("discount_pct_whole", F.ceil("discount_pct"))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("NONE")))
)

latest = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

ranked = (
    latest
    .withColumn("rank_in_group_2", F.row_number().over(w2))
    .filter(F.col("rank_in_group_2") == 1)
    .drop("rank_in_group_2")
)

ranked_promotions = (
    ranked
    .withColumn("rank_in_group_2", F.row_number().over(w))
    .filter(F.col("rank_in_group_2") == 1)
    .drop("rank_in_group_2")
)

enriched = (
    ranked_promotions
    .withColumn("prev_discount_pct", F.lag("discount_pct", 2).over(w3))
    .withColumn("running_discount_pct", F.sum("discount_pct").over(w4))
    .withColumn("cumulative_discount_pct_whole", F.sum("discount_pct_whole").over(w5))
)

result = (
    enriched
    .groupBy("channel")
    .pivot("promo_code", ["WELCOME5", "VIP"])
    .agg(F.avg("running_discount_pct").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
