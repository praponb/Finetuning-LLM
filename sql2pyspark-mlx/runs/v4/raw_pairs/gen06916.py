from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("device").isin(["desktop", "tablet"]))
    .select("browser")
    .distinct()
)
