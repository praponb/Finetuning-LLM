from pyspark.sql import functions as F

result = (
    df_sessions
    .select("customer_id")
    .intersect(
        df_sessions
        .filter(~F.col("country").isin(["DE", "SG"]))
        .select("customer_id")
    )
)
