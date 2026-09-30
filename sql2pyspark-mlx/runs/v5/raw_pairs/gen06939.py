from pyspark.sql import functions as F

result = (
    df_sessions
    .select("id", "browser")
    .exceptAll(
        df_sessions
        .filter(F.col("country") == "DE")
        .select("id", "browser")
    )
)
