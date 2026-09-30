from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        "country",
        F.when(F.col("age") >= 59, "high").when(F.col("age") >= 28, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("segment"))).alias("segment_values"),
        F.avg("credit_limit").alias("avg_credit_limit"),
    )
    .filter(F.col("avg_credit_limit") > 15800)
    .orderBy("avg_credit_limit", "country", "age_bucket")
    .limit(20)
)
