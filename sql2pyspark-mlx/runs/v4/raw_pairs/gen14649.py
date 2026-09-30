from pyspark.sql import functions as F

base = (
    df_customers
    .filter((F.col("segment").isin(["Consumer", "Corporate", "SMB"]) & F.col("country").isin(["UK", "JP", "DE"])) | F.col("segment").isNotNull())
    .withColumn("segment_prefix", F.substring("segment", 1, 4))
)

agg = (
    base
    .groupBy("segment")
    .agg(F.max("signup_date").alias("last_signup_date"))
)

result = (
    agg
    .orderBy(F.col("last_signup_date").desc(), "segment")
    .limit(10)
)
