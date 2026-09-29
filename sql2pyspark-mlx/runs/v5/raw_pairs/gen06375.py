from pyspark.sql import functions as F

result = (
    df_customers
    .select("country", F.lit("customer").alias("kind"))
    .unionByName(
        df_sessions
        .filter(F.col("browser") != "Firefox")
        .select("country", F.lit("session").alias("kind"))
    )
    .orderBy("country")
)
