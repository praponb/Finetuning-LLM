from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("key").orderBy("key_length", "id", "key")
w2 = Window.partitionBy("key").orderBy("prev_key_length", "id", "key")

lines = df_app_logs.select("id", F.explode("context").alias("key", "v"))

base = (
    lines
    .filter(F.col("key").isin(["region", "version", "trace"]))
    .withColumn("key_length", F.length("key"))
)

with_windows = (
    base
    .withColumn("next_key_length", F.lead("key_length").over(w_hist))
    .withColumn("prev_key_length", F.lag("key_length").over(w_hist))
    .withColumn("row_seq", F.row_number().over(w_hist))
)

scored = (
    with_windows
    .withColumn("seq_no", F.row_number().over(w_hist))
    .withColumn("prev_next_key_length", F.lag("next_key_length", 1).over(w2))
)

result = (
    scored
    .select(
        "id",
        "key",
        "v",
        "key_length",
        "next_key_length",
        "prev_next_key_length",
        F.when(F.col("key").rlike("^(version|region)$"), "OTHER_ZONE").when(F.col("key").rlike("^(trace)$"), "CORE").otherwise("GROUP_A").alias("key_zone"),
    )
    .orderBy(F.col("key_length").desc())
)
