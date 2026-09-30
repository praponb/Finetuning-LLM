from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount_pct", "id")
w2 = Window.partitionBy("channel").orderBy("discount_pct", "id")
w3 = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w4 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_promotions
    .filter(F.year("ends_on") == 2025)
    .select(
        "id",
        "channel",
        "starts_on",
        "discount_pct",
        F.ntile(5).over(w).alias("discount_pct_bucket"),
        F.row_number().over(w2).alias("event_seq"),
        F.lag("discount_pct", 1).over(w3).alias("prev_discount_pct"),
        F.first("channel", ignorenulls=True).over(w4).alias("first_known_channel"),
    )
)

result = (
    scored
    .select(
        "id",
        "channel",
        "discount_pct",
        "discount_pct_bucket",
        "event_seq",
        "prev_discount_pct",
        "first_known_channel",
        F.when(F.col("channel").rlike("^(store|partner)$"), "GROUP_B").otherwise("EXTENDED").alias("channel_zone"),
    )
    .orderBy(F.col("discount_pct_bucket").asc_nulls_last())
)
