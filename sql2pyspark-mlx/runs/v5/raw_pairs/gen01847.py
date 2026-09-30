from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "service").orderBy("status_code_high", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("service").orderBy(F.col("status_code_high").desc_nulls_first())
w3 = Window.orderBy(F.col("status_code").desc(), "id")

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "status_code",
        "context",
        F.col("context").getItem("region").alias("region"),
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "service",
        "status_code",
        "region",
        "user_tier",
        F.first("region", ignorenulls=True).over(w).alias("first_known_region"),
        F.rank().over(w2).alias("status_code_high_rank"),
        F.row_number().over(w3).alias("seq_no"),
    )
)
