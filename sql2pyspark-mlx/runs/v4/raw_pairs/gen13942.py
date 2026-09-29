from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("discount_pct").desc())

result = (
    df_promotions
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)
