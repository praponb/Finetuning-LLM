from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "service")
w2 = Window.partitionBy("service")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    flattened
    .filter(F.col("k") == "trace")
    .select(
        "service",
        "log_ts",
        "status_code",
        "k",
        "value",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share_2"),
    )
)
