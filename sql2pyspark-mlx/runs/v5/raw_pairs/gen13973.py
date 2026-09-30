from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("host").orderBy(F.col("log_ts").desc(), "id", "attr_key")
w2 = Window.partitionBy("attr_key", "level").orderBy(F.col("status_code").desc(), "id", "attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    flattened
    .filter(~F.col("level").isin(["WARN", "ERROR"]))
    .withColumn("event_seq", F.row_number().over(w_grp))
    .withColumn("seq_no", F.row_number().over(w2))
)
