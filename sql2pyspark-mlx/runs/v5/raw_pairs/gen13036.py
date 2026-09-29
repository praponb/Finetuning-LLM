from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("log_ts").desc(), "id", "k")
w_hist = Window.partitionBy("service").orderBy(F.col("log_ts").desc(), "id", "k")
w3 = Window.partitionBy("level").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("service").orderBy("log_ts", "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    exploded
    .filter(F.col("level") == "INFO")
    .select(
        "id",
        "k",
        F.row_number().over(w_grp).alias("row_seq"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.last("k", ignorenulls=True).over(w3).alias("last_known_k"),
        F.row_number().over(w_running).alias("event_seq_2"),
        F.row_number().over(w_running).alias("seq_no"),
    )
)
