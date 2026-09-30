from pyspark.sql import functions as F

valid_customers = (
    df_customers
    .filter(F.length("email") < 8)
    .withColumn("adjusted_credit_limit", F.round((F.col("credit_limit") * 100), 2))
)

age_bucket_summary = (
    valid_customers
    .groupBy(
        F.when(F.col("age") >= 57, "high").when(F.col("age") >= 31, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.sort_array(F.collect_set("segment")).alias("segment_list"))
)

result = (
    age_bucket_summary
    .select("age_bucket", "segment_list")
    .orderBy("age_bucket")
)
