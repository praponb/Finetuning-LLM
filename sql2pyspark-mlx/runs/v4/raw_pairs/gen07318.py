from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") <= 15400)
    .groupBy("is_active")
    .agg(
        F.sum(F.when(F.col("segment") == "Consumer", F.col("age")).otherwise(0)).alias("consumer_age"),
        F.concat_ws(", ", F.sort_array(F.collect_set("segment"))).alias("segment_values"),
    )
    .filter(F.col("consumer_age") > 22)
)
