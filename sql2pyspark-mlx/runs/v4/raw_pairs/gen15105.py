from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("has_fault").desc_nulls_last())
w2 = Window.partitionBy("host", "attr_key").orderBy(F.col("row_num").desc(), "id", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

flattened_attrs = (
    lines
    .select(
        "id",
        "host",
        "status_code",
        "attr_key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)

ranked = (
    flattened_attrs
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

filtered = (
    ranked
    .filter(F.col("row_num") != 1)
    .select(
        "id",
        "host",
        "attr_key",
        "status_code_high",
        "has_fault",
        "row_num",
        F.greatest("has_fault", "status_code").alias("greatest_has_fault_status_code"),
        F.when(F.col("has_fault") >= 1, "High").when(F.col("has_fault") >= 0, "Medium").otherwise("Low").alias("has_fault_tier"),
    )
)

ranked_app_logs = (
    filtered
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

result = (
    ranked_app_logs
    .withColumn("greatest_has_fault_status_code_tier", F.when(F.col("greatest_has_fault_status_code") >= 68, "LARGE").otherwise("SMALL"))
    .withColumn("has_fault_tier", F.when(F.col("has_fault") >= 1, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["attr_key", "greatest_has_fault_status_code_tier", "has_fault_tier"], ["attr_key", "greatest_has_fault_status_code_tier"], []],
        "attr_key",
        "greatest_has_fault_status_code_tier",
        "has_fault_tier",
    )
    .agg(
        F.count_if(F.col("host") == "h3").alias("h3_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("host") == "h1", F.col("has_fault")).otherwise(0)).alias("h1_has_fault"),
    )
)
