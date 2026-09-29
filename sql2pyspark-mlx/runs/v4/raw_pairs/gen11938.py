from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("status_code", "id", "k")
w_hist = Window.orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("host").orderBy("next_status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("k")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

with_windows = (
    exploded
    .withColumn("next_status_code", F.lead("status_code", 1).over(w))
    .withColumn("last_known_k", F.last("k", ignorenulls=True).over(w_hist))
)

with_windows_app_logs = (
    with_windows
    .select(
        "*",
        F.sum("next_status_code").over(w3).alias("running_next_status_code"),
        F.stddev_samp("status_code").over(w4).alias("k_stddev_samp_status_code"),
    )
)

result = (
    with_windows_app_logs
    .select(
        "id",
        "service",
        "host",
        "k",
        "next_status_code",
        "running_next_status_code",
        "k_stddev_samp_status_code",
        F.when(F.col("k").rlike("^(version|region)$"), "GROUP_A").when(F.col("k").rlike("^(trace)$"), "GROUP_B").otherwise("EXTENDED").alias("k_zone"),
    )
    .orderBy(F.col("k").desc())
)
