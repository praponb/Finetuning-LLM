from pyspark.sql import functions as F

base = (
    df_sessions
    .filter(F.col("browser").isNotNull() & F.col("browser").isNotNull())
    .withColumn("is_mobile", F.when(F.col("device") == "mobile", 1).otherwise(0))
)

result = (
    base
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.city"), F.col("b.name"))
)
