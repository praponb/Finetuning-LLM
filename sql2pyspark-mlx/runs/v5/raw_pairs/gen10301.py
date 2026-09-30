from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("k").orderBy("status_code_high", "id", "k")
w_ordered = Window.partitionBy("k").orderBy("has_fault", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k").orderBy("prev_has_fault", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("k").orderBy("last_known_status_code_high", "id", "k")

flattened = (
    df_app_logs
    .select("id", "status_code", F.explode("context").alias("k", "attr_value"))
)

parsed = (
    flattened
    .select(
        "id",
        "status_code",
        "k",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

with_windows = (
    parsed
    .filter(F.col("k") == "trace")
    .select(
        "id",
        "status_code",
        "k",
        "has_fault",
        F.lag("status_code_high").over(w_hist).alias("prev_status_code_high"),
        F.lag("has_fault").over(w_hist).alias("prev_has_fault"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.last("status_code_high", ignorenulls=True).over(w_ordered).alias("last_known_status_code_high"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "status_code",
        "k",
        "prev_has_fault",
        F.min("has_fault").over(w3).alias("running_min_has_fault"),
        F.max("has_fault").over(w3).alias("running_max_has_fault"),
        F.avg("has_fault").over(w3).alias("running_avg_has_fault"),
        F.lead("prev_has_fault", 1).over(w4).alias("next_prev_has_fault"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("prev_has_fault") >= 1, "HIGH").otherwise("LOW").alias("prev_has_fault_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.sort_array(F.collect_set("k")).alias("k_list_2"),
        F.count(F.when(F.col("k") == "version", 1)).alias("version_count"),
    )
    .orderBy(F.col("version_count").asc_nulls_first())
)
