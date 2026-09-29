from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "country",
        F.trim("device").alias("clean_device"),
        F.initcap(F.trim("device")).alias("device_title"),
    )
)
