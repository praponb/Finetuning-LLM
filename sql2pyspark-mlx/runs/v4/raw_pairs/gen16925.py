from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("promo_code")

metrics = (
    df_promotions
    .withColumn("discount_pct_bucket", F.ntile(5).over(w))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4))
)

result = (
    metrics
    .select("id", "channel", "promo_code", "ends_on", "discount_pct_bucket")
    .orderBy(F.col("id").desc_nulls_first())
)
