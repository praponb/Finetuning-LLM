from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Edge", "Firefox", "Chrome"]) & F.col("device").isin(["tablet", "desktop", "mobile"]) & (F.col("browser") != "Firefox"))
    .select("id", "browser")
)
