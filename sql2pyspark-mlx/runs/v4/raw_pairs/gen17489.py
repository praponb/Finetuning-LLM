from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("host").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("attr_key").orderBy(F.col("status_code").desc(), "id", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

result = (
    lines
    .withColumn("cumulative_status_code", F.sum("status_code").over(w))
    .withColumn("running_status_code", F.sum("status_code").over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
)
