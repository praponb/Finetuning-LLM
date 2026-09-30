from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country").isin(["UK", "TH"]))
    .select("id", "browser", "device")
)
