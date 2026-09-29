from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active") & F.col("segment").isin(["Corporate", "SMB"]))
    .withColumn("segment_city", F.concat("segment", F.lit(", "), "city"))
    .withColumn("age_plus_credit_limit", (F.col("age") + F.col("credit_limit")))
    .orderBy("id")
    .limit(5)
)
