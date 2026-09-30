from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("level").orderBy(F.col("num_context").desc())
w2 = Window.partitionBy("level").orderBy("num_context", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("level").orderBy("num_context", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "context",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.size("context").alias("num_context"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.col("context")["version"].alias("version"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.cume_dist().over(w_part).alias("num_context_cume_dist"),
        F.avg("status_code_high").over(w2).alias("status_code_high_avg_5_prior"),
        F.last("status_code_high", ignorenulls=True).over(w3).alias("last_known_status_code_high"),
    )
)
