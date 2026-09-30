from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("starts_on").desc(), "id")

ranked = (
    df_promotions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .select("id", "channel", "promo_code", "starts_on", "ends_on", "discount_pct")
    .orderBy(F.col("promo_code").asc_nulls_first())
)
