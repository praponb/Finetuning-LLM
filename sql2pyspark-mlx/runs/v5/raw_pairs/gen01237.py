from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("status_code_high", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("level").orderBy("status_code_high", "id")
w3 = Window.partitionBy("level").orderBy(F.col("status_code_high").desc_nulls_first())

parsed = (
    df_app_logs
    .filter(F.col("service") != "checkout")
    .select(
        "id",
        "service",
        "level",
        "context",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.coalesce(F.col("context").getItem("version"), F.lit("unknown")).alias("version_or_default"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.coalesce(F.col("context")["region"], F.lit("none")).alias("region_or_default"),
    )
)

result = (
    parsed
    .filter(F.col("status_code_high") >= 1)
    .select(
        "id",
        "user_id",
        "version_or_default",
        F.avg("status_code_high").over(w).alias("status_code_high_avg_5_prior"),
        F.lead("status_code_high", 1).over(w2).alias("next_status_code_high"),
        F.cume_dist().over(w3).alias("status_code_high_cume_dist"),
    )
)
