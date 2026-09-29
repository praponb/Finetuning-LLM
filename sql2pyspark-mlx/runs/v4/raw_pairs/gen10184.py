from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct", "id")
w2 = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_promotions
    .withColumn("discount_pct_bucket", F.ntile(4).over(w))
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w2))
)
