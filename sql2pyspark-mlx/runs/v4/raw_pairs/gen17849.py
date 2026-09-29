from pyspark.sql import functions as F

result = (
    df_web_events
    .select("page")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec").isNotNull())
        .select("page")
    )
)
