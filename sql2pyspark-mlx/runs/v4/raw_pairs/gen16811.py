from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("activity_month").orderBy(F.col("discount_pct_per_partner").desc())

scored = (
    df_promotions
    .select(
        "*",
        F.first("channel", ignorenulls=True).over(w).alias("first_known_channel"),
        F.last("channel", ignorenulls=True).over(w_recent).alias("last_known_channel"),
    )
)

result = (
    scored
    .groupBy(
        F.date_trunc("MONTH", "starts_on").alias("activity_month"),
        F.when(F.col("discount_pct") >= 0.36, "HIGH").otherwise("LOW").alias("discount_pct_tier"),
    )
    .agg(
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("discount_pct_per_partner"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("discount_pct").alias("total_discount_pct"),
    )
    .filter(F.col("total_discount_pct") >= 0)
    .withColumn("discount_pct_per_partner_rank", F.rank().over(w3))
    .orderBy(F.col("activity_month").asc_nulls_last())
)
