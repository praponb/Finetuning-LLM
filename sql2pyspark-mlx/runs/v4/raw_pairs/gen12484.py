from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("level").orderBy("status_code_high", "id", "attr_key")
w3 = Window.partitionBy("attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

flattened_attrs = (
    exploded
    .select(
        "id",
        "level",
        "attr_key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
    )
)

with_windows = (
    flattened_attrs
    .select(
        "*",
        F.sum("has_fault").over(w).alias("level_sum_has_fault"),
        F.lag("has_fault", 2).over(w2).alias("prev_has_fault"),
        F.sum("has_fault").over(w3).alias("attr_key_sum_has_fault"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "attr_key",
        "status_code_high",
        "has_fault",
        "level_sum_has_fault",
        "prev_has_fault",
        "attr_key_sum_has_fault",
    )
    .orderBy(F.col("attr_key").desc_nulls_last())
)
