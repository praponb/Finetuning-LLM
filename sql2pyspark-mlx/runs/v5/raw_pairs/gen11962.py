from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("status_code", "id", "attr_key")

lines = (
    df_app_logs
    .select("id", "level", "status_code", F.explode("context").alias("attr_key", "v"))
)

result = (
    lines
    .filter(F.col("status_code") == 267)
    .select(
        "id",
        "level",
        "status_code",
        "attr_key",
        "v",
        F.lag("status_code").over(w).alias("prev_status_code"),
        F.row_number().over(w).alias("row_seq"),
    )
)
