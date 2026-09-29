from pyspark.sql import functions as F

result = (
    df_sessions
    .select("browser")
    .intersectAll(
        df_sessions
        .filter(F.col("country") == "SG")
        .select("browser")
    )
)
