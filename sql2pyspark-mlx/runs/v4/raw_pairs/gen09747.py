from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id")
w4 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy("discount_pct", "id")

scored = (
    df_promotions
    .select(
        "*",
        F.min("discount_pct").over(w).alias("channel_min_discount_pct"),
        F.avg("discount_pct").over(w2).alias("discount_pct_avg_3_prior"),
        F.ntile(5).over(w3).alias("discount_pct_bucket"),
        F.last("channel", ignorenulls=True).over(w4).alias("last_known_channel"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

result = (
    scored
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct_avg_3_prior",
        "discount_pct_bucket",
        "last_known_channel",
        "seq_no",
        F.when(F.col("promo_code").rlike("^(SPRING10|WELCOME5)$"), "EXTENDED").otherwise("GROUP_B").alias("promo_code_zone"),
    )
    .orderBy(F.col("promo_code").asc_nulls_last())
)
