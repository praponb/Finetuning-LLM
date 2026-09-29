from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("channel").orderBy(F.col("starts_on").desc(), "id")
w_running = Window.orderBy("discount_pct")
w3 = Window.partitionBy("channel", "promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running_2 = Window.partitionBy("promo_code").orderBy("starts_on", "id")
w5 = Window.partitionBy("promo_code", "channel").orderBy(F.col("discount_pct").desc(), "id")
w6 = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("promo_code", "channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("discount_pct_rank").desc_nulls_first())
w9 = Window.orderBy(F.col("discount_pct").desc())
w10 = Window.partitionBy("promo_code", "channel").orderBy(F.col("seq_no").desc_nulls_last())

with_windows = (
    df_promotions
    .select(
        "*",
        F.row_number().over(w_grp).alias("row_seq"),
        F.rank().over(w_running).alias("discount_pct_rank"),
        F.first("channel", ignorenulls=True).over(w3).alias("first_known_channel"),
        F.lag("discount_pct", 2, 0).over(w_running_2).alias("prev_discount_pct"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

scored = (
    with_windows
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w6))
    .withColumn("last_known_channel_2", F.last("channel", ignorenulls=True).over(w7))
)

remaining_keys = (
    scored
    .select("promo_code", "channel")
    .intersect(
        df_promotions
        .filter(F.col("starts_on") < "2023-11-01")
        .select("promo_code", "channel")
    )
)

verified = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "*",
        F.rank().over(w8).alias("discount_pct_rank_rank"),
        F.rank().over(w9).alias("discount_pct_rank_2"),
        F.cume_dist().over(w10).alias("seq_no_cume_dist"),
    )
)
