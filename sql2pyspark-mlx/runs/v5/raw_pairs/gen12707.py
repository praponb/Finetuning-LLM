from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("quarter_start").orderBy(F.col("num_rows").desc())

enriched = (
    df_promotions
    .select(
        "id",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.min("discount_pct").over(w).alias("last4_min_discount_pct"),
        F.sum("discount_pct").over(w).alias("last4_sum_discount_pct"),
        F.max("discount_pct").over(w).alias("last4_max_discount_pct"),
    )
)

result = (
    enriched
    .groupBy(
        F.date_trunc("QUARTER", "ends_on").alias("quarter_start"),
        F.when(F.col("last4_sum_discount_pct") >= 1.37, "PREMIUM").otherwise("STANDARD").alias("last4_sum_discount_pct_tier"),
    )
    .agg(
        F.round(F.avg("last4_max_discount_pct"), 2).alias("avg_last4_max_discount_pct"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
    )
    .withColumn("num_rows_percent_rank", F.percent_rank().over(w2))
)
