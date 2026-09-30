from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("status_code", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("service")
w3 = Window.orderBy("log_ts", "id")

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.coalesce(F.col("context").getItem("version"), F.lit("none")).alias("version_or_default"),
        F.col("context")["version"].alias("version"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "service",
        "log_ts",
        "version_or_default",
        F.avg("status_code").over(w).alias("status_code_avg_6"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
        F.lead("status_code", 1).over(w3).alias("next_status_code"),
    )
)
