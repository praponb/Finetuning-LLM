from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("host", "level").orderBy(F.col("log_ts").desc(), "id", "key")
w_ordered = Window.partitionBy("level").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select("id", "host", "log_ts", "level", F.explode("context").alias("key", "value"))
)

result = (
    lines
    .select(
        "id",
        "log_ts",
        "level",
        "key",
        "value",
        F.row_number().over(w_running).alias("seq_no"),
        F.first("key", ignorenulls=True).over(w_ordered).alias("first_known_key"),
        F.first("key", ignorenulls=True).over(w_ordered).alias("first_known_key_2"),
    )
)
