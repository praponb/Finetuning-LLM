from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").desc(), "id", "attr_key")
w2 = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.first("attr_key", ignorenulls=True).over(w2).alias("first_known_attr_key"),
        F.last("attr_key", ignorenulls=True).over(w2).alias("last_known_attr_key"),
        F.row_number().over(w3).alias("row_seq_2"),
        F.first("attr_key", ignorenulls=True).over(w2).alias("first_known_attr_key_2"),
    )
)
