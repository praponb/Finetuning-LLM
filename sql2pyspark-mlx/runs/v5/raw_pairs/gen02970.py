from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("ends_on").desc(), "id")

cleaned = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2023-11-29").cast("date")) & (F.col("promo_code") == "SPRING10"))
    .select(
        "*",
        F.date_format("starts_on", "MMM yyyy").alias("starts_on_label"),
        F.date_format("ends_on", "MMM yyyy").alias("ends_on_label"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

result = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "promo_code", "ends_on_label", "channel_filled")
)
