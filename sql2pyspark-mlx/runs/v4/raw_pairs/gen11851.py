from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("status_code")
w2 = Window.partitionBy("host", "level")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = (
    flattened
    .withColumn("status_code_percent_rank", F.percent_rank().over(w))
    .withColumn("host_sum_status_code", F.sum("status_code").over(w2))
)
