from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("key", "service").orderBy("status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("status_code").desc(), "id", "key")
w4 = Window.partitionBy("service").orderBy(F.col("status_code").desc())
w5 = Window.partitionBy("service", "key").orderBy(F.col("status_code").desc(), "id", "key")

lines = (
    df_app_logs
    .select("id", "service", "status_code", F.explode("context").alias("key", "value"))
)

result = (
    lines
    .select(
        "id",
        "service",
        "status_code",
        "key",
        F.last("key", ignorenulls=True).over(w_hist).alias("last_known_key"),
        F.first("key", ignorenulls=True).over(w2).alias("first_known_key"),
        F.row_number().over(w3).alias("seq_no"),
        F.percent_rank().over(w4).alias("status_code_percent_rank"),
        F.row_number().over(w5).alias("seq_no_2"),
    )
)
