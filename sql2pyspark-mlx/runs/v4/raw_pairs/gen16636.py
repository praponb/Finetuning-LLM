from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

flattened = (
    df_app_logs
    .select("id", "service", "log_ts", F.explode_outer("context").alias("k", "v"))
)

valid = (
    flattened
    .filter(F.col("k").isNotNull() & (F.col("service") == "search"))
    .select("*", F.length("service").alias("service_length"))
)

base = (
    valid
    .filter(F.col("k") == "trace")
    .withColumn("service_group", F.when(F.col("service").isin(["search", "auth"]), "Core").otherwise("Other"))
)

remaining_keys = (
    base
    .select("id", "service")
    .subtract(
        df_app_logs
        .filter(F.col("log_ts") >= "2024-05-01")
        .select("id", "service")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("service_length_max_15m", F.max("service_length").over(w))
    .withColumn("service_length_count_30m", F.count("service_length").over(w2))
)
