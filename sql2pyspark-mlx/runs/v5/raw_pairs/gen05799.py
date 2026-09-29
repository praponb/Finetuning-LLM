from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("ends_on").desc(), "id")
w2 = Window.partitionBy("channel").orderBy("discount_pct_whole", "id")
w_running = Window.partitionBy("channel").orderBy(F.col("discount_pct_whole").desc(), "id")
w4 = Window.orderBy("discount_pct_whole", "id")
w5 = Window.partitionBy("channel").orderBy("discount_pct_whole_bucket", "id")

base = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2023-09-29").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "ends_on",
        F.ceil("discount_pct").alias("discount_pct_whole"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

ranked = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "channel", "discount_pct_whole", "resolved_channel")
)

metrics = (
    ranked
    .select(
        "id",
        "channel",
        F.lead("discount_pct_whole", 2, 0).over(w2).alias("next_discount_pct_whole"),
        F.row_number().over(w_running).alias("row_seq"),
        F.ntile(4).over(w4).alias("discount_pct_whole_bucket"),
    )
)

ranked_promotions = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked_promotions
    .select(
        "id",
        "channel",
        "next_discount_pct_whole",
        "row_seq",
        "discount_pct_whole_bucket",
        F.when(F.col("channel").rlike("^(partner|web)$"), "GROUP_B").otherwise("PRIORITY").alias("channel_zone"),
    )
)
