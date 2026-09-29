from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(F.countDistinct("sqft").alias("unique_citys"))
    .orderBy("unique_citys", "opened_cohort")
    .limit(10)
)
