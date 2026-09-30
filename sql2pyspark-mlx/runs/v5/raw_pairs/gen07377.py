from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter((F.col("city") == "Tokyo") & (F.col("sqft") >= 7977) & (F.col("city") == "Chiang Mai"))
)

agg = (
    recent_stores
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(F.min("opened_date").alias("first_opened_date"))
)

result = agg.select("opened_cohort", "first_opened_date")
