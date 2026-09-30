from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "key").orderBy("status_code")
w2 = Window.partitionBy("host", "key")
w3 = Window.partitionBy("level")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    lines
    .filter(F.col("key") == "trace")
    .select(
        "host",
        "log_ts",
        "status_code",
        F.cume_dist().over(w).alias("status_code_cume_dist"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
        F.max("status_code").over(w3).alias("level_max_status_code"),
    )
)
