from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "browser",
        "country",
        "device",
        F.trim("country").alias("clean_country"),
    )
)
