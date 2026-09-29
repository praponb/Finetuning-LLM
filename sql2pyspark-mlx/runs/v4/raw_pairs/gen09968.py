from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("promo_code").orderBy("discount_pct", "id")

metrics = (
    df_promotions
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w_hist))
    .withColumn("discount_pct_bucket", F.ntile(3).over(w_part))
    .withColumn("prev_discount_pct", F.lag("discount_pct", 1).over(w_part))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("discount_pct") >= 0.31, "LARGE").otherwise("SMALL").alias("discount_pct_tier"),
    )
    .agg(
        F.sum(F.when(F.col("channel") == "mobile", F.col("discount_pct_bucket")).otherwise(0)).alias("mobile_discount_pct_bucket"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.avg("prev_discount_pct").alias("avg_prev_discount_pct"),
        F.sum("discount_pct").alias("total_discount_pct"),
        F.percentile_approx("discount_pct", 0.9).alias("discount_pct_p90"),
        F.percentile_approx("discount_pct_bucket", 0.9).alias("discount_pct_bucket_p90"),
    )
    .orderBy(F.col("distinct_channels").asc_nulls_first())
)
