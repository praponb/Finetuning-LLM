from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("attr_key").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("service").orderBy("status_code", "id", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

result = (
    lines
    .filter(F.col("host").isin(["h1", "h2"]))
    .select(
        "id",
        "host",
        "level",
        "status_code",
        "attr_key",
        F.count("status_code").over(w_grp).alias("running_count_status_code"),
        F.sum("status_code").over(w_grp).alias("running_sum_status_code"),
        F.min("status_code").over(w_grp).alias("running_min_status_code"),
        F.lead("status_code", 1, 0).over(w2).alias("next_status_code"),
    )
)
