from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key", "level").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level", "host")
w3 = Window.partitionBy("level").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
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
    exploded
    .withColumn("last_known_attr_key", F.last("attr_key", ignorenulls=True).over(w))
    .withColumn("level_count_status_code", F.count("status_code").over(w2))
    .withColumn("last_known_attr_key_2", F.last("attr_key", ignorenulls=True).over(w3))
)
