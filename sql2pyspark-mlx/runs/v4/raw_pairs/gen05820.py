from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy("discount_pct", "id")
w4 = Window.partitionBy("promo_code", "channel").orderBy(F.col("discount_pct").desc())
w5 = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_promotions
    .withColumn("discount_pct_avg_6_prior", F.avg("discount_pct").over(w))
    .withColumn("running_discount_pct", F.sum("discount_pct").over(w2))
    .withColumn("prev_discount_pct", F.lag("discount_pct", 1).over(w3))
    .withColumn("discount_pct_percent_rank", F.percent_rank().over(w4))
    .withColumn("running_discount_pct_2", F.sum("discount_pct").over(w5))
)

result = (
    scored
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct_avg_6_prior",
        "prev_discount_pct",
        F.when(F.col("promo_code").rlike("^(SPRING10|VIP)$"), "PRIORITY").otherwise("GROUP_B").alias("promo_code_zone"),
    )
    .orderBy(F.col("promo_code").asc_nulls_first())
)
