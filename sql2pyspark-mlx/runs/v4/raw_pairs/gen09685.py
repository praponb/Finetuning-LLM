from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("channel").orderBy("discount_pct", "id")
w_ordered = Window.partitionBy("channel").orderBy("starts_on", "id")
w_ordered_2 = Window.partitionBy("promo_code", "channel")

metrics = (
    df_promotions
    .withColumn("discount_pct_bucket", F.ntile(3).over(w_hist))
    .withColumn("next_discount_pct", F.lead("discount_pct", 1).over(w_ordered))
    .withColumn("promo_code_min_discount_pct", F.min("discount_pct").over(w_ordered_2))
)

result = (
    metrics
    .select("id", "promo_code", "starts_on", "next_discount_pct")
    .orderBy(F.col("next_discount_pct").desc())
)
