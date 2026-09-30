from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(9, 16))
    .select("country")
    .unionByName(
        df_sessions
        .filter(F.col("country").isin(["DE", "JP", "UK"]))
        .select("country")
    )
    .orderBy(F.col("country").desc())
)
