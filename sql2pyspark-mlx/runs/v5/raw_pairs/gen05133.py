from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isNull())
    .select("browser", "country")
    .intersect(
        df_sessions
        .filter(F.col("country") == "US")
        .select("browser", "country")
    )
    .orderBy(F.col("country").desc())
)
