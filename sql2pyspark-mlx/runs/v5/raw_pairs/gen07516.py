from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        (F.max("age") - F.min("age")).alias("age_range"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
    )
    .orderBy(F.col("avg_credit_limit").desc(), "city_values")
)
