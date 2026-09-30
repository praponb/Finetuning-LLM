from pyspark.sql import functions as F

result = (
    df_web_events
    .select("id")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec") > 470)
        .select("id")
    )
)
