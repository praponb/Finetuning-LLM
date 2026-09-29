from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")

latest = (
    df_promotions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "channel",
        "ends_on",
        "discount_pct",
        F.when(F.col("channel").rlike("^(store|web)$"), "EXTENDED").when(F.col("channel").rlike("^(mobile|partner)$"), "GROUP_A").otherwise("GROUP_B").alias("channel_zone"),
    )
    .orderBy(F.col("id").asc_nulls_first(), F.col("channel").desc())
)
