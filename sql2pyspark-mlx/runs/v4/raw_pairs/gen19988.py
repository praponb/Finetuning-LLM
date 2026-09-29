from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code")

metrics = (
    df_promotions
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w)), 4))
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w2))
    .withColumn("promo_code_max_discount_pct", F.max("discount_pct").over(w3))
)

result = (
    metrics
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        "discount_pct_share",
        F.when(F.col("channel").rlike("^(mobile|web)$"), "EXTENDED").otherwise("GROUP_A").alias("channel_zone"),
    )
    .orderBy("discount_pct_share")
)
