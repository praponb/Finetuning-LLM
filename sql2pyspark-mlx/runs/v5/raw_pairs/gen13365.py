from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.to_date("started_at") >= "2023-04-04")
    .select("id", "country", "browser")
    .subtract(
        df_sessions
        .filter(F.col("device").isin(["mobile", "desktop"]))
        .select("id", "country", "browser")
    )
)
