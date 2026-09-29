from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "tier").orderBy("status_code", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("level").orderBy("status_code", "id")

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "status_code",
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.col("context")["region"].alias("region"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.avg("status_code").over(w).alias("status_code_avg_3_prior"),
        F.lag("status_code", 2).over(w2).alias("prev_status_code"),
    )
)
