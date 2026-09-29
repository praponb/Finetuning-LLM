from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select("country", "browser")
    .subtract(
        df_sessions
        .filter(~F.col("browser").isin(["Firefox", "Safari"]))
        .select("country", "browser")
    )
    .orderBy(F.col("country").desc())
)
