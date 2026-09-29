from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on", "id")

deduped = (
    df_promotions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select("id", "promo_code", "starts_on", "ends_on", "discount_pct", "rn")
    .orderBy(F.col("promo_code").desc())
)
