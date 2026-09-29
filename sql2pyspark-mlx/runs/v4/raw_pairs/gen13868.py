from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel")
w2 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id")

metrics = (
    df_promotions
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w)), 4))
    .withColumn("discount_pct_bucket", F.ntile(5).over(w2))
)

result = (
    metrics
    .select("id", "promo_code", "starts_on", "discount_pct", "discount_pct_bucket")
)
