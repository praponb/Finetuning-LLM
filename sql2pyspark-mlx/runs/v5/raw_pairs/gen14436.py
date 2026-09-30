from pyspark.sql import functions as F

result = (
    df_sessions
    .select("id", "country")
    .exceptAll(
        df_sessions
        .filter(F.to_date("started_at") >= "2024-02-27")
        .select("id", "country")
    )
)
