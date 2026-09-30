from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
        "is_active",
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.min("age").alias("min_age"),
        F.sum("age").alias("total_age"),
    )
    .filter(F.col("total_age") > 160)
    .drop("total_age")
)
