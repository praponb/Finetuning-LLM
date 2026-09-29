from pyspark.sql import functions as F

result = (
    df_customers
    .agg(
        F.min("credit_limit").alias("min_credit_limit"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
    )
)
