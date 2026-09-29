from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct", "id")
w2 = Window.partitionBy("promo_code").orderBy("starts_on", "id")
w3 = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel", "promo_code").orderBy(F.col("ends_on").desc(), "id")
w_running = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_promotions
    .withColumn("next_discount_pct", F.lead("discount_pct", 1).over(w))
    .withColumn("next_discount_pct_2", F.lead("discount_pct", 2).over(w2))
    .withColumn("cumulative_discount_pct", F.sum("discount_pct").over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w_running))
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
        "next_discount_pct_2",
        "cumulative_discount_pct",
        "first_known_channel",
        F.when(F.col("promo_code").rlike("^(BUNDLE|WELCOME5)$"), "CORE").when(F.col("promo_code").rlike("^(SPRING10|VIP)$"), "GROUP_A").otherwise("EXTENDED").alias("promo_code_zone"),
    )
)
