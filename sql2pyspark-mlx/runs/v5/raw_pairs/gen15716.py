from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("starts_on", "id")
w2 = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("channel", "promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_promotions
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.avg("discount_pct").over(w2).alias("discount_pct_avg_2"),
        F.sum("discount_pct").over(w3).alias("running_discount_pct"),
    )
)

result = (
    metrics
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "discount_pct_avg_2",
        "running_discount_pct",
        F.when(F.col("channel").rlike("^(web|partner)$"), "OTHER_ZONE").when(F.col("channel").rlike("^(mobile|store)$"), "GROUP_A").otherwise("GROUP_B").alias("channel_zone"),
    )
    .orderBy(F.col("id").desc(), F.col("promo_code").desc())
    .limit(10)
)
