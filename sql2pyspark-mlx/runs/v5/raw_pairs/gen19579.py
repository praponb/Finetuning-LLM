from pyspark.sql import functions as F

valid_customers = (
    df_customers
    .filter(F.col("segment") == "Consumer")
    .withColumn("segment_email_key", F.concat_ws(",", "segment", "email"))
)

summary = (
    valid_customers
    .groupBy("city")
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.countDistinct("credit_limit").alias("unique_citys"),
    )
)

result = summary.select("city", "avg_credit_limit", "unique_citys")
