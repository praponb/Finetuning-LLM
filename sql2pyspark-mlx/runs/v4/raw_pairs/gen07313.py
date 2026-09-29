from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w2 = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.lower("promo_code").alias("promo_code_lower"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

result = (
    filtered
    .withColumn("next_discount_pct", F.lead("discount_pct", 1, 0).over(w))
    .withColumn("discount_pct_avg_2_prior", F.avg("discount_pct").over(w2))
    .withColumn("cumulative_discount_pct", F.sum("discount_pct").over(w3))
)
