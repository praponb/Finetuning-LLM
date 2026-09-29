from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("browser").isin(["Firefox", "Chrome"]))
    .select("id", "country")
)
