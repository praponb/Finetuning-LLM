from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["desktop", "mobile", "tablet"]))
    .select("device", "country")
    .distinct()
)
