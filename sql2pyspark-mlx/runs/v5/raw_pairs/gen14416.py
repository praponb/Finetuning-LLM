from pyspark.sql import functions as F

result = (
    df_sessions
    .select("browser", "device")
    .subtract(
        df_sessions
        .filter(F.col("country").isin(["US", "TH", "UK"]))
        .select("browser", "device")
    )
)
