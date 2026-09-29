from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on", "id")

base = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2023-10-06").cast("date")) & (F.col("ends_on") <= "2025-08-08"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.date_format("starts_on", "yyyy-MM-dd").alias("starts_on_label"),
    )
)

result = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
