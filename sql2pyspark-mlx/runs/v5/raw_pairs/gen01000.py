from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter(F.col("store_name").like("%Downt%") & F.col("store_name").isNotNull())
)

opened_cohort_summary = (
    recent_stores
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        "city",
    )
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.count("*").alias("num_stores"),
    )
)

result = (
    opened_cohort_summary
    .filter(F.col("num_stores") < 7)
    .select("opened_cohort", "city", "num_stores")
    .orderBy(F.col("num_stores").desc(), "city", "opened_cohort")
    .limit(5)
)
