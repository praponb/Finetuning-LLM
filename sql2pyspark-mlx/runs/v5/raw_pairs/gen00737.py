from pyspark.sql import functions as F

result = (
    df_web_events
    .select("id")
    .intersectAll(
        df_web_events
        .filter(F.col("duration_sec").between(290, 357))
        .select("id")
    )
)
