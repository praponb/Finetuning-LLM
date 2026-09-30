from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("starts_on", "id")

ranked = (
    df_promotions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "rank_in_group",
        F.when(F.col("channel").rlike("^(web|store)$"), "GROUP_B").when(F.col("channel").rlike("^(mobile|partner)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("channel_zone"),
    )
    .orderBy(F.col("starts_on").asc_nulls_last())
)
