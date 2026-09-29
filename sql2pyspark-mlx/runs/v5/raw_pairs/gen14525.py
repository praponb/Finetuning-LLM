from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w3 = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code", "channel").orderBy(F.col("ends_on").desc(), "id")
w5 = Window.partitionBy("channel")
w6 = Window.partitionBy("channel").orderBy(F.col("running_max_prev_discount_pct").desc())

metrics = (
    df_promotions
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w))
    .withColumn("prev_discount_pct", F.lag("discount_pct", 1, 0).over(w2))
)

with_windows = (
    metrics
    .filter(F.year("starts_on") >= 2025)
    .withColumn("running_sum_prev_discount_pct", F.sum("prev_discount_pct").over(w3))
    .withColumn("running_avg_prev_discount_pct", F.avg("prev_discount_pct").over(w3))
    .withColumn("running_max_prev_discount_pct", F.max("prev_discount_pct").over(w3))
)

cleaned = (
    with_windows
    .filter(F.col("promo_code") == "SPRING10")
    .select(
        "*",
        F.col("discount_pct").cast("int").alias("discount_pct_int"),
        F.when(F.col("channel").isin(["web", "partner"]), "Tier 1").otherwise("Other").alias("channel_group"),
    )
)

deduped = (
    cleaned
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "running_sum_prev_discount_pct",
        "running_max_prev_discount_pct",
        "discount_pct_int",
        "channel_group",
    )
)

with_windows_promotions = (
    deduped
    .select(
        "*",
        F.round((F.col("running_sum_prev_discount_pct") / F.sum("running_sum_prev_discount_pct").over(w5)), 4).alias("running_sum_prev_discount_pct_share"),
        F.avg("running_sum_prev_discount_pct").over(w5).alias("channel_avg_running_sum_prev_discount_pct"),
        F.rank().over(w6).alias("running_max_prev_discount_pct_rank"),
    )
)

result = (
    with_windows_promotions
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "running_sum_prev_discount_pct",
        "discount_pct_int",
        "channel_group",
        "channel_avg_running_sum_prev_discount_pct",
        F.when(F.col("channel").rlike("^(web|store)$"), "PRIORITY").when(F.col("channel").rlike("^(mobile|partner)$"), "GROUP_B").otherwise("GROUP_A").alias("channel_zone"),
    )
    .orderBy(F.col("starts_on").desc_nulls_last())
)
