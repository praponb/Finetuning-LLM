from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_promotions
    .withColumn("discount_pct_bucket", F.ntile(5).over(w_recent))
    .withColumn("running_discount_pct", F.sum("discount_pct").over(w2))
)

result = (
    metrics
    .select("id", "channel", "starts_on", "ends_on", "running_discount_pct")
    .orderBy(F.col("starts_on").desc())
)
