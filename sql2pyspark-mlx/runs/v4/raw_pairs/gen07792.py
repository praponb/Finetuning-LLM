from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("log_ts", "id", "attr_key")
w2 = Window.orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    flattened
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("first_known_attr_key", F.first("attr_key", ignorenulls=True).over(w2))
)
