from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Firefox", "Chrome"]) & (F.col("browser").isin(["Firefox", "Edge", "Chrome"]) | F.col("country").isNull()))
    .select("customer_id", F.trim("browser").alias("clean_browser"))
    .orderBy(F.col("customer_id").desc())
)
