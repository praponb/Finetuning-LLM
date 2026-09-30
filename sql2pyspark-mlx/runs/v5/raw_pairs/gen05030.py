from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.orderBy("log_ts", "id").rowsBetween(-5, Window.currentRow)

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "context",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.col("context")["version"].alias("version"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.count("status_code_high").over(w).alias("status_code_high_count_1h"),
        F.avg("status_code_high").over(w2).alias("status_code_high_avg_5"),
    )
)
