from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_flagged").desc(), "id", "k")
w_part = Window.partitionBy("k").orderBy("status_code_high", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("is_flagged", "id", "k")
w4 = Window.orderBy("is_flagged", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("k", "service").orderBy("status_code_high", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("k").orderBy("cumulative_status_code", "id", "k")
w7 = Window.partitionBy("service")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

unpacked = (
    flattened
    .filter(F.col("status_code") > 324)
    .select(
        "id",
        "service",
        "status_code",
        "k",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

enriched = (
    unpacked
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("cumulative_status_code", F.sum("status_code").over(w_part))
    .withColumn("is_flagged_bucket", F.ntile(3).over(w3))
    .withColumn("cumulative_status_code_2", F.sum("status_code").over(w4))
)

enriched_app_logs = (
    enriched
    .select(
        "*",
        F.first("is_flagged_bucket", ignorenulls=True).over(w5).alias("first_known_is_flagged_bucket"),
        F.lead("status_code_high", 2).over(w6).alias("next_status_code_high"),
        F.round((F.col("cumulative_status_code_2") / F.sum("cumulative_status_code_2").over(w7)), 4).alias("cumulative_status_code_2_share"),
    )
)

result = (
    enriched_app_logs
    .select(
        "id",
        "k",
        "status_code_high",
        "row_seq",
        "first_known_is_flagged_bucket",
        "next_status_code_high",
        "cumulative_status_code_2_share",
    )
    .orderBy(F.col("k").desc_nulls_first())
)
