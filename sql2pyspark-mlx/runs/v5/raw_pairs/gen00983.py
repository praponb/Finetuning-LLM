from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on", "id")
w2 = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("discount_pct", "id")

base = (
    df_promotions
    .filter(~F.col("channel").eqNullSafe(F.lit("partner")))
    .withColumn("promo_code_channel_key", F.concat_ws(",", "promo_code", "channel"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("NONE")))
)

result = (
    base
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "promo_code_channel_key",
        F.row_number().over(w).alias("seq_no"),
        F.last("channel", ignorenulls=True).over(w2).alias("last_known_channel"),
        F.lead("discount_pct", 1, 0).over(w3).alias("next_discount_pct"),
    )
)
