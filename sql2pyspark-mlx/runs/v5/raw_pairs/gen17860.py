from pyspark.sql import functions as F

result = (
    df_sessions
    .select("id")
    .subtract(
        df_sessions
        .filter(F.to_date("started_at") < "2024-06-01")
        .select("id")
    )
)
