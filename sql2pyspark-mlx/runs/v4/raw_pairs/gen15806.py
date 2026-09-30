from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("segment") != "Consumer") & ((F.col("segment") != "Consumer") | F.col("name").like("%Wong%")))
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.avg("age").alias("avg_age"),
    )
    .orderBy(F.col("avg_age").desc())
)
