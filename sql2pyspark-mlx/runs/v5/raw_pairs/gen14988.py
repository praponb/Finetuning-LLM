from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "attr_key").orderBy("status_code", "id", "attr_key")
w2 = Window.partitionBy("service").orderBy("status_code", "id", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

deduped = (
    lines
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "service", "status_code", "attr_key")
)

latest = (
    deduped
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("id", "service", "attr_key")
)

cleaned = (
    latest
    .filter(F.col("attr_key").isNotNull() & (~F.col("attr_key").eqNullSafe(F.lit("region"))))
    .select("*", F.upper("attr_key").alias("attr_key_upper"))
)

summary = (
    cleaned
    .groupBy("service", "attr_key")
    .agg(
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.size(F.collect_set("service")).alias("num_services"),
        F.count_if(F.col("attr_key") == "trace").alias("trace_events"),
        F.countDistinct("service").alias("distinct_services"),
    )
)

result = (
    summary
    .select(
        "service",
        "attr_key",
        "attr_key_list",
        "num_services",
        "trace_events",
        "distinct_services",
    )
    .orderBy(F.col("num_services").desc())
)
