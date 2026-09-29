from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser") == "Chrome")
    .select("id", "device")
    .unionByName(
        df_sessions
        .filter(~F.col("browser").isin(["Firefox", "Safari"]))
        .select("id", "device")
    )
    .distinct()
    .orderBy(F.col("device").desc())
)
