from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("browser").isin(["Safari", "Firefox", "Edge"]))
    .select("id", "country")
    .orderBy("country")
)
