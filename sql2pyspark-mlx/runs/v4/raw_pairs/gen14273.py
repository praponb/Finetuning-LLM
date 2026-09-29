from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(10, 16) & F.col("customer_id").isin([2, 14, 15]))
    .withColumn("country_title", F.initcap(F.trim("country")))
    .withColumn("started_at_period", F.date_trunc("MONTH", "started_at"))
)
