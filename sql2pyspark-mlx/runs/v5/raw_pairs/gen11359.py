from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("has_fault_whole", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("attr_key").orderBy(F.col("status_code_high").desc_nulls_last())
w3 = Window.orderBy(F.col("has_fault").desc(), "id", "attr_key")
w4 = Window.partitionBy("attr_key").orderBy("has_fault", "id", "attr_key")
w5 = Window.partitionBy("attr_key").orderBy("status_code_high_cume_dist")
w_hist = Window.partitionBy("attr_key").orderBy("cumulative_has_fault_whole", "id", "attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

unpacked = (
    flattened
    .filter(F.col("attr_key").isin(["region", "trace"]))
    .select(
        "id",
        "attr_key",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
    )
)

valid = (
    unpacked
    .filter(F.col("status_code_high").between(1, 2))
    .select(
        "id",
        "attr_key",
        "attr_value",
        "status_code_high",
        "has_fault",
        F.floor("has_fault").alias("has_fault_whole"),
        F.when(F.col("has_fault") >= 1, "High").when(F.col("has_fault") >= 0, "Medium").otherwise("Low").alias("has_fault_level"),
    )
)

with_windows = (
    valid
    .filter(F.col("status_code_high") <= 2)
    .select(
        "*",
        F.sum("has_fault_whole").over(w).alias("cumulative_has_fault_whole"),
        F.cume_dist().over(w2).alias("status_code_high_cume_dist"),
        F.ntile(5).over(w3).alias("has_fault_bucket"),
        F.lag("status_code_high", 1).over(w4).alias("prev_status_code_high"),
    )
)

latest = (
    with_windows
    .withColumn("rn", F.rank().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "attr_key",
        "attr_value",
        "has_fault_whole",
        "cumulative_has_fault_whole",
        "status_code_high_cume_dist",
        "prev_status_code_high",
    )
)

metrics = (
    latest
    .select(
        "id",
        "attr_key",
        "attr_value",
        F.lead("status_code_high_cume_dist").over(w_hist).alias("next_status_code_high_cume_dist"),
        F.lead("has_fault_whole").over(w_hist).alias("next_has_fault_whole"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)

result = (
    metrics
    .select(
        "id",
        "attr_key",
        "attr_value",
        "next_status_code_high_cume_dist",
        "next_has_fault_whole",
        F.when(F.col("attr_key").rlike("^(trace|version)$"), "PRIORITY").otherwise("GROUP_B").alias("attr_key_zone"),
    )
)
