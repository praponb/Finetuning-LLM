from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("channel").orderBy("discount_pct", "id")
w_hist = Window.partitionBy("channel").orderBy("ends_on_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_promotions
    .filter(F.col("ends_on") >= F.lit("2023-10-19").cast("date"))
    .select("*", F.last_day("ends_on").alias("ends_on_month_end"))
)

result = (
    valid
    .select(
        "id",
        "promo_code",
        "ends_on",
        "ends_on_month_end",
        F.first("channel", ignorenulls=True).over(w).alias("first_known_channel"),
        F.lag("discount_pct", 1).over(w_recent).alias("prev_discount_pct"),
        F.sum("discount_pct").over(w_hist).alias("cumulative_discount_pct"),
    )
)
