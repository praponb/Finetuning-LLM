from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isin([5, 30, 33]))
    .select(
        "country",
        F.when(F.col("browser") == "Firefox", 1).otherwise(0).alias("is_firefox"),
        F.to_date("started_at").alias("started_at_date"),
    )
)
