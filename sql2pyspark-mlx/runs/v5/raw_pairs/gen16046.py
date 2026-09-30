from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(F.date_trunc("QUARTER", "ends_on").alias("quarter_start"))
    .agg(
        F.min("discount_pct").alias("min_discount_pct"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("promo_code") == "SPRING10", 1)).alias("spring10_count"),
    )
    .orderBy(F.col("distinct_ids").asc_nulls_last())
)
