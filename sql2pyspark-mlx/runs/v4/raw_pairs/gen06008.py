from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("channel", "promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("starts_on").desc(), "id")

result = (
    df_promotions
    .withColumn("discount_pct_bucket", F.ntile(5).over(w))
    .withColumn("running_discount_pct", F.sum("discount_pct").over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)
