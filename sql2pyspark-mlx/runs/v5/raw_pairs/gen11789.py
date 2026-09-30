from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct").desc())

latest = (
    df_promotions
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "channel", "promo_code", "starts_on", "ends_on", "discount_pct")
)

result = (
    latest
    .withColumn("week_start", F.date_trunc("WEEK", "ends_on"))
    .rollup("week_start")
    .agg(
        F.countDistinct("channel").alias("distinct_channels"),
        F.count("*").alias("num_rows"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("num_rows").desc_nulls_last())
)
