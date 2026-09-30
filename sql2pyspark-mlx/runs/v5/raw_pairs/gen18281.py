from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNotNull() & F.col("city").isin(["Singapore", "Chiang Mai"]))
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("region") == "South", 1).otherwise(0))) / F.count("*")), 1).alias("pct_south"),
    )
    .orderBy(F.col("pct_south").desc(), "opened_month", "opened_cohort")
    .limit(1)
)
