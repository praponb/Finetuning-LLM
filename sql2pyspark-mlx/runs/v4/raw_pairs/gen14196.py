from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("age") >= 61, "high").when(F.col("age") >= 37, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.max("age").alias("max_age"),
    )
    .filter(F.col("max_age") >= 47)
)
