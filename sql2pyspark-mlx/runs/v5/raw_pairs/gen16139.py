from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("discount_pct", "id")
w3 = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(-3, -1)

scored = (
    df_promotions
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        F.first("channel", ignorenulls=True).over(w).alias("first_known_channel"),
        F.lead("discount_pct", 2).over(w2).alias("next_discount_pct"),
        F.avg("discount_pct").over(w3).alias("discount_pct_avg_3_prior"),
    )
)

result = (
    scored
    .select(
        "id",
        "starts_on",
        "first_known_channel",
        "next_discount_pct",
        "discount_pct_avg_3_prior",
    )
    .orderBy(F.col("id").asc_nulls_last())
)
