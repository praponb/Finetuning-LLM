from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id")

ranked = (
    df_promotions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .select("id", "channel", "promo_code", "starts_on", "discount_pct", "row_num")
    .orderBy(F.col("channel").desc_nulls_first(), F.col("starts_on").desc())
)
