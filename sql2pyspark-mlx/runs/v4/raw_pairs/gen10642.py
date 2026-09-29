from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("starts_on", "id").rowsBetween(-6, -1)
w3 = Window.orderBy("starts_on", "id").rowsBetween(-2, -1)

cleaned = (
    df_promotions
    .filter((F.col("ends_on") > F.to_date(F.lit("2024-01-13"))) & F.col("channel").isNotNull())
    .withColumn("ends_on_month", F.month("ends_on"))
    .withColumn("channel_promo_code_key", F.concat_ws("-", "channel", "promo_code"))
)

result = (
    cleaned
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w))
    .withColumn("ends_on_month_avg_6_prior", F.avg("ends_on_month").over(w2))
    .withColumn("ends_on_month_avg_2_prior", F.avg("ends_on_month").over(w3))
)
