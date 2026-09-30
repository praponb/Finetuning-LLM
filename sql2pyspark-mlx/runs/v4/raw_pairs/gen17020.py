from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sum(F.when(F.col("segment") == "Consumer", F.col("credit_limit")).otherwise(0)).alias("consumer_credit_limit"),
    )
    .filter(F.col("consumer_credit_limit") >= 36)
    .select("signup_month", "city_list", "consumer_credit_limit")
    .orderBy(F.col("consumer_credit_limit").desc())
)
