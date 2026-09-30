from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("service").orderBy(F.col("log_ts").desc(), "id")
w3 = Window.partitionBy("service").orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "payload_json",
        F.col("context")["trace"].alias("trace"),
        F.coalesce(F.col("context").getItem("version"), F.lit("unknown")).alias("version_or_default"),
    )
)

result = (
    parsed
    .withColumn("last_known_trace", F.last("trace", ignorenulls=True).over(w_grp))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("last_known_trace_2", F.last("trace", ignorenulls=True).over(w3))
)
