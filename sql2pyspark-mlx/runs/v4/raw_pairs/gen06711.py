from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("discount_pct").desc())
w3 = Window.orderBy("discount_pct", "id")
w4 = Window.orderBy("ends_on_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("promo_code").orderBy("ends_on_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2024-03-27").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.when(F.col("discount_pct") >= 0.29, "High").when(F.col("discount_pct") >= 0.18, "Medium").otherwise("Low").alias("discount_pct_band"),
        F.add_months("ends_on", 12).alias("ends_on_plus_months"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

result = (
    filtered
    .withColumn("running_discount_pct", F.sum("discount_pct").over(w))
    .withColumn("discount_pct_rank", F.rank().over(w2))
    .withColumn("prev_discount_pct", F.lag("discount_pct", 1).over(w3))
    .withColumn("running_discount_pct_2", F.sum("discount_pct").over(w4))
    .withColumn("cumulative_discount_pct", F.sum("discount_pct").over(w5))
)
