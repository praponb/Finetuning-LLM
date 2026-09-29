from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~((F.col("browser") == "Edge") | (F.col("customer_id") == 34)))
    .select("started_at", "browser")
    .orderBy(F.col("browser").desc())
)
