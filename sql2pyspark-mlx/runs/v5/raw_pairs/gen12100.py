from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.sum(F.when(F.col("segment") == "Consumer", 1).otherwise(0)).alias("consumer_count"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .filter(F.col("consumer_count") > 41)
    .orderBy("consumer_count")
)
