from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("key").orderBy(F.col("status_code").desc(), "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    flattened
    .filter(F.col("key") == "trace")
    .withColumn("prev_status_code", F.lag("status_code").over(w))
    .withColumn("next_status_code", F.lead("status_code").over(w))
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("status_code_bucket", F.ntile(5).over(w2))
)
