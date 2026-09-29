from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("host").orderBy(F.col("status_code").desc())
w3 = Window.partitionBy("host", "key").orderBy("status_code", "id", "key")

lines = (
    df_app_logs
    .select("id", "host", "status_code", F.explode("context").alias("key", "value"))
)

result = (
    lines
    .select(
        "id",
        "key",
        F.percent_rank().over(w_recent).alias("status_code_percent_rank"),
        F.rank().over(w2).alias("status_code_rank"),
        F.lead("status_code", 2).over(w3).alias("next_status_code"),
    )
)
