from pyspark.sql import functions as F

result = (
    df_customers
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 15100, "LARGE").otherwise("SMALL"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "signup_date"))
    .rollup("credit_limit_tier", "quarter_start")
    .agg(
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("credit_limit_per_new_york"),
        F.sum(F.when(F.col("segment") == "Corporate", F.col("credit_limit")).otherwise(0)).alias("corporate_credit_limit"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count_if(F.col("city") == "Chiang Mai").alias("chiang_mai_events"),
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("age_per_singapore"),
        F.avg(F.when(F.col("segment") == "SMB", F.col("credit_limit"))).alias("smb_avg_credit_limit"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("smb_avg_credit_limit") < 1000000)
    .orderBy("age_per_singapore")
)
