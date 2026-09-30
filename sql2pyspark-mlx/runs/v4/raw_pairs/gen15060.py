from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(-3, Window.currentRow)
w_grp = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())

enriched = (
    df_promotions
    .withColumn("discount_pct_avg_3", F.avg("discount_pct").over(w))
    .withColumn("discount_pct_rank", F.rank().over(w_grp))
)

result = (
    enriched
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct",
        "discount_pct_avg_3",
        "discount_pct_rank",
        F.when(F.col("promo_code").rlike("^(SPRING10|WELCOME5)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("promo_code_zone"),
    )
    .orderBy(F.col("discount_pct").desc_nulls_last())
)
