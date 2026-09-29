from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("channel").orderBy("days_ends_on_to_starts_on", "id")
w2 = Window.partitionBy("channel").orderBy(F.col("days_ends_on_to_starts_on").asc_nulls_last())

filtered = (
    df_promotions
    .filter(F.col("ends_on") >= F.to_date(F.lit("2023-07-10")))
    .select(
        "id",
        "channel",
        "ends_on",
        "discount_pct",
        F.date_trunc("month", "ends_on").alias("ends_on_period"),
        F.datediff("starts_on", "ends_on").alias("days_ends_on_to_starts_on"),
    )
)

result = (
    filtered
    .filter(F.to_date("ends_on_period") >= "2025-05-04")
    .withColumn("days_ends_on_to_starts_on_bucket", F.ntile(10).over(w_part))
    .withColumn("days_ends_on_to_starts_on_bucket_2", F.ntile(10).over(w_part))
    .withColumn("days_ends_on_to_starts_on_rank", F.rank().over(w2))
)
