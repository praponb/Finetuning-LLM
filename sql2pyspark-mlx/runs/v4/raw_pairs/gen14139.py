from pyspark.sql import functions as F

result = (
    df_sessions
    .select("browser")
    .subtract(
        df_sessions
        .filter(F.col("country").isNull())
        .select("browser")
    )
)
