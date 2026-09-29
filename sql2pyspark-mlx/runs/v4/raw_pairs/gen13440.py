from pyspark.sql import functions as F

src = df_customers.filter(F.col("city").isNotNull())

customers_stats = (
    src
    .groupBy("segment")
    .agg(
        F.count("age").alias("age_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)

result = (
    customers_stats
    .filter(F.col("age_count") < 9)
    .orderBy("segment", F.col("age_count").desc())
    .limit(1)
)
