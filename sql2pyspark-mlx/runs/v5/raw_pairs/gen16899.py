from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Chrome", "Safari"]) & (F.col("country") != "US"))
    .select("country", "browser")
)
