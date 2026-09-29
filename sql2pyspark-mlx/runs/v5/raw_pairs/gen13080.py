from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date", F.lit("customer").alias("record_type"))
    .union(df_stores.select("opened_date", F.lit("store").alias("record_type")))
    .distinct()
    .orderBy(F.col("record_type").desc())
)
