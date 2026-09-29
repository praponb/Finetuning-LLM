from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("browser").isin(["Safari", "Chrome", "Edge"]))
    .select("device")
    .unionByName(
        df_sessions
        .filter(F.col("started_at") >= "2024-09-01")
        .select("device")
    )
    .orderBy("device")
)
