from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("ends_on").desc(), "id")
w3 = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_promotions
    .select(
        "id",
        "channel",
        "ends_on",
        F.last("channel", ignorenulls=True).over(w).alias("last_known_channel"),
        F.row_number().over(w2).alias("row_seq"),
        F.last("channel", ignorenulls=True).over(w3).alias("last_known_channel_2"),
    )
)
