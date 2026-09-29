from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") < "2023-03-03")
    .rollup(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        "region",
    )
    .agg(F.min("opened_date").alias("first_opened_date"))
)
