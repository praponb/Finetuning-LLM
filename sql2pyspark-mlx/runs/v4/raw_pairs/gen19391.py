from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("QUARTER", "signup_date").alias("quarter_start"))
    .agg(
        F.sum("credit_limit").alias("total_credit_limit"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.stddev_samp("age").alias("age_stddev"),
        F.avg("credit_limit").alias("avg_credit_limit"),
        F.max(F.when(F.col("country") == "JP", F.col("age"))).alias("jp_max_age"),
    )
    .orderBy(F.col("age_stddev").desc_nulls_first())
)
