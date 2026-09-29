from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("browser") == "Safari") & (F.col("country") == "SG"))
    .withColumn("browser_filled", F.coalesce("browser", F.lit("N/A")))
    .withColumn("started_at_year", F.year("started_at"))
)
