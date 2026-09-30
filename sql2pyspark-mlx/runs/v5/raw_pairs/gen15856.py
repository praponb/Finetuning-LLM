from pyspark.sql import functions as F

cleaned = (
    df_customers
    .filter(F.col("city").isNotNull())
    .withColumn("credit_limit_int", F.col("credit_limit").cast("int"))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("credit_limit") >= 16000, "LARGE").otherwise("SMALL").alias("credit_limit_tier"),
        "city",
    )
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count_if(F.col("country") == "SG").alias("sg_events"),
        F.count_if(F.col("segment") == "Corporate").alias("corporate_events"),
    )
)
