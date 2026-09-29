from pyspark.sql import functions as F

result = (
    df_sessions
    .select("id", "browser")
    .intersectAll(
        df_sessions
        .filter(F.col("device") != "desktop")
        .select("id", "browser")
    )
)
