from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("age") >= 71, "high").when(F.col("age") >= 36, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.sum("credit_limit").alias("total_credit_limit"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
    )
    .orderBy("total_credit_limit", "stddev_credit_limit")
)
