from pyspark.sql import functions as F

result = (
    df_web_events
    .select("page", "id")
    .intersectAll(
        df_web_events
        .filter(F.col("duration_sec").between(168, 279))
        .select("page", "id")
    )
)
