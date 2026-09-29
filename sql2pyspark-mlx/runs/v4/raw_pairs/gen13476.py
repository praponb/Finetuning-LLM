from pyspark.sql import functions as F

result = (
    df_promotions
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.35, "PREMIUM").otherwise("STANDARD"))
    .withColumn("week_start", F.date_trunc("WEEK", "ends_on"))
    .groupingSets(
        [["discount_pct_tier", "week_start"], ["discount_pct_tier"]],
        "discount_pct_tier",
        "week_start",
    )
    .agg(
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_rows"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
    )
    .filter(F.col("store_rows") < 1)
    .orderBy("discount_pct_tier")
)
