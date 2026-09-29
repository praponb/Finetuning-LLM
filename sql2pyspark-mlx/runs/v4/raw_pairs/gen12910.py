from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("starts_on", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("channel")
w3 = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w4 = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_promotions
    .filter(F.col("ends_on").between("2024-09-01", "2025-10-01"))
    .select(
        "*",
        F.when(F.col("discount_pct") >= 0.32, "High").when(F.col("discount_pct") >= 0.11, "Medium").otherwise("Low").alias("discount_pct_level"),
    )
)

result = (
    base
    .withColumn("discount_pct_avg_3", F.avg("discount_pct").over(w))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4))
    .withColumn("next_discount_pct", F.lead("discount_pct", 1).over(w3))
    .withColumn("running_discount_pct", F.sum("discount_pct").over(w4))
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w5))
)
