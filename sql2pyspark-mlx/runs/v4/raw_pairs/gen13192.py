from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code")
w2 = Window.orderBy("discount_pct", "id")

filtered = (
    df_promotions
    .filter(F.col("starts_on") >= F.lit("2024-02-05").cast("date"))
    .withColumn("ends_on_month", F.month("ends_on"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("UNKNOWN")))
)

result = (
    filtered
    .select(
        "*",
        F.max("ends_on_month").over(w).alias("promo_code_max_ends_on_month"),
        F.lead("discount_pct", 1, 0).over(w2).alias("next_discount_pct"),
    )
)
