from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(
        F.when(F.col("discount_pct") >= 0.36, "LARGE").otherwise("SMALL").alias("discount_pct_tier"),
        "promo_code",
    )
    .agg(
        F.sum(F.when(F.col("channel") == "partner", F.col("discount_pct")).otherwise(0)).alias("partner_discount_pct"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("partner_discount_pct").asc_nulls_first())
)
