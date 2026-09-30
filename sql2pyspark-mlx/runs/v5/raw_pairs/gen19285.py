from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy(F.col("ends_on").desc(), "id")
w3 = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.last("channel", ignorenulls=True).over(w).alias("last_known_channel"),
        F.row_number().over(w2).alias("event_seq"),
        F.sum("discount_pct").over(w3).alias("cumulative_discount_pct"),
    )
)
