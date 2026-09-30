from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.to_date(F.lit("2025-09-01"))) & F.col("temp_c").isNotNull())
    .select("*", F.initcap(F.trim("device_code")).alias("device_code_title"))
)

flagged = (
    base
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .withColumn("session_id_tier", F.when(F.col("session_id") >= 8, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["device_code", "session_id_tier"], ["device_code"], []],
        "device_code",
        "session_id_tier",
    )
    .agg(
        F.min("reading_ts").alias("first_seen"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_count"),
    )
    .orderBy(F.col("device_code").desc())
)
