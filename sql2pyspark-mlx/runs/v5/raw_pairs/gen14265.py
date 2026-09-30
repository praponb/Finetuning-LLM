from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("log_ts").desc())
w2 = Window.partitionBy("k").orderBy("row_num", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k").orderBy(F.col("log_ts").desc(), "id", "k")
w4 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = df_app_logs.select("id", "log_ts", F.explode("context").alias("k", "attr_value"))

deduped = (
    flattened
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

with_windows = deduped.withColumn("first_known_k", F.first("k", ignorenulls=True).over(w2))

deduped_app_logs = (
    with_windows
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped_app_logs
    .select(
        "id",
        "k",
        "attr_value",
        "row_num",
        F.row_number().over(w3).alias("seq_no"),
        F.first("k", ignorenulls=True).over(w4).alias("first_known_k_2"),
    )
)
