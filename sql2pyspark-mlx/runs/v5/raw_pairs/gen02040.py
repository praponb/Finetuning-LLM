from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "service").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        F.explode("context").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .filter(F.col("service").isin(["checkout", "search", "billing"]))
    .select(
        "id",
        "host",
        "log_ts",
        "k",
        F.first("k", ignorenulls=True).over(w).alias("first_known_k"),
    )
)
