from pyspark.sql import functions as F

result = (
    df_customers
    .cube("segment")
    .agg(
        F.bit_and("age").alias("age_bit_and"),
        F.stddev_samp("credit_limit").alias("credit_limit_stddev"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("age").alias("min_age"),
        F.sum(F.when(F.col("segment") == "SMB", F.col("age")).otherwise(0)).alias("smb_age"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("credit_limit_stddev").asc_nulls_first())
)
