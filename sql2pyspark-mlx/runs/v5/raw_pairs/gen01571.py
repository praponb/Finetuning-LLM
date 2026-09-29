from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("days_ends_on_to_starts_on", "id")

cleaned = (
    df_promotions
    .filter(F.col("ends_on") >= F.lit("2023-06-18").cast("date"))
    .withColumn("days_ends_on_to_starts_on", F.datediff("starts_on", "ends_on"))
)

result = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
