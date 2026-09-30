from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") < "2024-04-18")
    .select("id", "browser")
    .unionByName(
        df_sessions
        .filter(F.col("browser") == "Firefox")
        .select("id", "browser")
    )
    .orderBy(F.col("browser").desc())
)
