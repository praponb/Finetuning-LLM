from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("latency_ms", "id", "attr_key")
w2 = Window.partitionBy("host").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

result = (
    flattened
    .select(
        "*",
        F.lag("latency_ms", 1).over(w).alias("prev_latency_ms"),
        F.last("attr_key", ignorenulls=True).over(w2).alias("last_known_attr_key"),
    )
)
