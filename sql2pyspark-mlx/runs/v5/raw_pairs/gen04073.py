from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc())

ranked = (
    df_promotions
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .select("id", "channel", "promo_code", "ends_on", "discount_pct", "row_num")
    .orderBy(F.col("discount_pct").asc_nulls_last(), F.col("row_num").desc())
)
