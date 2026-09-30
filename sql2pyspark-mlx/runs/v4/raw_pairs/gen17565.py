from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter((F.col("city") == "Tokyo") & (~F.col("region").isin(["North", "Central", "South"]) | (F.col("sqft") >= 13038)))
)

opened_cohort_summary = (
    recent_stores
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(F.max("sqft").alias("max_sqft"))
)

result = (
    opened_cohort_summary
    .select("opened_cohort", "max_sqft")
    .orderBy("opened_cohort")
    .limit(50)
)
