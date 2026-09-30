from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").isNull())
    .groupBy("segment")
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.avg("age").alias("avg_age"),
        F.max("age").alias("max_age"),
    )
    .filter(F.col("max_age") < 67)
    .orderBy("avg_age")
)
