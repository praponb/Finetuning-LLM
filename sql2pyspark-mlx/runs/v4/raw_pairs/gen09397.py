from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Edge", "Safari", "Firefox"]))
    .select("id", "device")
    .intersect(
        df_sessions
        .filter(F.col("browser") == "Firefox")
        .select("id", "device")
    )
)
