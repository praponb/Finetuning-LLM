from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device") == "tablet")
    .select("country")
    .intersect(
        df_sessions
        .filter(F.col("started_at") >= "2023-06-01")
        .select("country")
    )
    .orderBy(F.col("country").desc())
)
