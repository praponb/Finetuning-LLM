from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Edge", "Chrome", "Firefox"]))
    .select(
        "customer_id",
        "country",
        "browser",
        F.lower("browser").alias("browser_lower"),
    )
    .orderBy("browser", F.col("country").desc())
)
