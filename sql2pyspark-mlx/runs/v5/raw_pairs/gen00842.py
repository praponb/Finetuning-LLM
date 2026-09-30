from pyspark.sql import functions as F

result = (
    df_sessions
    .select("device")
    .intersectAll(
        df_sessions
        .filter(F.col("country") == "US")
        .select("device")
    )
)
