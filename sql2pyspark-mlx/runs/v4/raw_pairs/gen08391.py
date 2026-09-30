from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("starts_on_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel", "promo_code")
w3 = Window.partitionBy("promo_code")

valid = (
    df_promotions
    .filter(F.col("ends_on") > F.lit("2023-07-25").cast("date"))
    .select(
        "*",
        F.length("channel").alias("channel_length"),
        F.add_months("starts_on", 1).alias("starts_on_plus_months"),
    )
)

result = (
    valid
    .select(
        "*",
        F.first("channel_length", ignorenulls=True).over(w).alias("first_known_channel_length"),
        F.count("channel_length").over(w2).alias("channel_count_channel_length"),
        F.sum("discount_pct").over(w3).alias("promo_code_sum_discount_pct"),
    )
)
