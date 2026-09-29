from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .filter(F.col("status_code").between(267, 289))
    .select(
        "id",
        "status_code",
        F.coalesce(F.col("context")["region"], F.lit("none")).alias("region_or_default"),
        F.col("context")["version"].alias("version"),
        F.col("context")["region"].alias("region"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    flattened_attrs
    .groupBy("tier")
    .agg(
        F.avg(F.when(F.col("tier") == "gold", F.col("status_code"))).alias("gold_avg_status_code"),
        F.count("*").alias("cnt"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("gold_avg_status_code") >= 0)
    .orderBy(F.col("tier").desc())
)
