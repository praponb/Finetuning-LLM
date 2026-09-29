from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("ends_on_plus_months", "id")
w2 = Window.orderBy("discount_pct", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy("ends_on_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("promo_code").orderBy("ends_on_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_promotions
    .filter(F.col("starts_on") > "2025-04-01")
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.add_months("ends_on", 12).alias("ends_on_plus_months"),
    )
)

result = (
    valid
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("discount_pct_avg_2", F.avg("discount_pct").over(w2))
    .withColumn("cumulative_discount_pct", F.sum("discount_pct").over(w3))
    .withColumn("running_discount_pct", F.sum("discount_pct").over(w4))
    .withColumn("running_discount_pct_2", F.sum("discount_pct").over(w5))
)
