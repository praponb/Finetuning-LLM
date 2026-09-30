from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on")

deduped = (
    df_promotions
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

result = deduped.select("id", "promo_code", "ends_on", "row_num")
