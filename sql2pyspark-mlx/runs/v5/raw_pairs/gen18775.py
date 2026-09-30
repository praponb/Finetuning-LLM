from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w2 = Window.partitionBy("promo_code", "channel")
w3 = Window.partitionBy("channel").orderBy("starts_on", "id")

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .withColumn("channel_title", F.initcap(F.trim("channel")))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("DIRECT")))
)

result = (
    filtered
    .withColumn("prev_discount_pct", F.lag("discount_pct", 1).over(w))
    .withColumn("promo_code_max_discount_pct", F.max("discount_pct").over(w2))
    .withColumn("prev_discount_pct_2", F.lag("discount_pct", 1, 0).over(w3))
)
