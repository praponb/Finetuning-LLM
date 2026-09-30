from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.orderBy("status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("status_code").desc(), "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    exploded
    .withColumn("service_sum_status_code", F.sum("status_code").over(w))
    .withColumn("running_status_code", F.sum("status_code").over(w2))
    .withColumn("status_code_bucket", F.ntile(5).over(w3))
)
