from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("status_code", "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "host",
        "level",
        "key",
        "attr_value",
        F.lead("status_code").over(w).alias("next_status_code"),
        F.lag("status_code").over(w).alias("prev_status_code"),
        F.row_number().over(w).alias("seq_no"),
    )
)
