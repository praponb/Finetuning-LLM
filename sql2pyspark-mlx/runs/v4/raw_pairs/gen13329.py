from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/home", "/search"]) & F.col("session_id").isin([1, 19, 22]))
    .select("id", "page")
)
