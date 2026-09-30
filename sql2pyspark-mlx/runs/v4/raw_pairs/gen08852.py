from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("log_ts", "id", "attr_key")
w_hist = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("host").orderBy("status_code")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

base = (
    lines
    .filter(F.col("attr_key").isNotNull())
    .withColumn("attr_key_upper", F.upper("attr_key"))
    .withColumn("resolved_attr_key", F.coalesce("attr_key", F.lit("OTHER")))
)

enriched = (
    base
    .select(
        "*",
        F.lag("status_code", 2, 0).over(w).alias("prev_status_code"),
        F.last("attr_key", ignorenulls=True).over(w_hist).alias("last_known_attr_key"),
        F.rank().over(w_grp).alias("status_code_rank"),
    )
)

result = (
    enriched
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "attr_key",
        "attr_key_upper",
        "prev_status_code",
        "last_known_attr_key",
    )
)
