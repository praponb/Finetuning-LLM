from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("num_context", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level").orderBy(F.col("num_context").asc_nulls_first())
w3 = Window.orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "context",
        F.coalesce(F.col("context").getItem("version"), F.lit("none")).alias("version_or_default"),
        F.col("context").getItem("region").alias("region"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        F.size("context").alias("num_context"),
        F.col("context")["version"].alias("version"),
    )
)

result = (
    unpacked
    .withColumn("cumulative_num_context", F.sum("num_context").over(w))
    .withColumn("num_context_dense_rank", F.dense_rank().over(w2))
    .withColumn("running_num_context", F.sum("num_context").over(w3))
)
