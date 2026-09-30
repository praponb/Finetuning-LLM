from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.filter(~F.col("city").isin(["Berlin", "London", "Bangkok"])).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("s.country").isin(["DE", "SG", "UK"]))
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.device"),
        F.col("s.customer_id"),
    )
)
