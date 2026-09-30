from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("days_starts_on_to_ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .withColumn("days_starts_on_to_ends_on", F.datediff("ends_on", "starts_on"))
    .withColumn("is_store", F.when(F.col("channel") == "store", 1).otherwise(0))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("DIRECT")))
)

result = (
    cleaned
    .withColumn("first_known_days_starts_on_to_ends_on", F.first("days_starts_on_to_ends_on", ignorenulls=True).over(w))
    .withColumn("cumulative_is_store", F.sum("is_store").over(w2))
)
