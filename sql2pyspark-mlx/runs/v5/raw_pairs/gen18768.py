from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("starts_on").desc(), "id")

deduped = (
    df_promotions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "rn",
        F.when(F.col("channel").rlike("^(mobile|store)$"), "PRIORITY").when(F.col("channel").rlike("^(web|partner)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("channel_zone"),
    )
)
