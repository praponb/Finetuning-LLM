from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "service")
w2 = Window.partitionBy("service", "level").orderBy(F.col("status_code").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .withColumn("k_max_status_code", F.max("status_code").over(w))
    .withColumn("status_code_rank", F.rank().over(w2))
)
