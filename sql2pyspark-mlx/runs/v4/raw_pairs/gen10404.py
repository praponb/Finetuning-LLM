from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select("id", "country", "customer_id")
    .subtract(
        df_sessions
        .filter(F.col("country").isin(["UK", "SG", "US"]))
        .select("id", "country", "customer_id")
    )
)
