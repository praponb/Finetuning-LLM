from pyspark.sql import functions as F

src = (
    df_stores
    .filter((F.col("city") == "Bangkok") & (F.col("region").isin(["South", "North", "West"]) | (F.year("opened_date") == 2025)))
)

summary = (
    src
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.count("*").alias("store_count"),
    )
)

result = summary.select("opened_cohort", "avg_sqft", "store_count")
