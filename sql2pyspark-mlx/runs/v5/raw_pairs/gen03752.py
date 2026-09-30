from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("attr_key").orderBy(F.col("status_code_high").desc_nulls_first())
w_grp = Window.orderBy(F.col("is_flagged").desc(), "id", "attr_key")
w3 = Window.partitionBy("attr_key").orderBy(F.col("status_code_high_rank").desc_nulls_last(), "id", "attr_key")
w4 = Window.partitionBy("attr_key").orderBy(F.col("is_flagged_bucket").desc(), "id", "attr_key")
w5 = Window.partitionBy("attr_key").orderBy("is_flagged_bucket", "id", "attr_key")

exploded = df_app_logs.select("id", "status_code", F.explode("context").alias("attr_key", "v"))

flattened_attrs = (
    exploded
    .select(
        "id",
        "attr_key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
    )
)

metrics = (
    flattened_attrs
    .select(
        "id",
        "attr_key",
        F.rank().over(w_part).alias("status_code_high_rank"),
        F.ntile(4).over(w_grp).alias("is_flagged_bucket"),
    )
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select("id", "attr_key", "status_code_high_rank", "is_flagged_bucket")
)

scored = (
    deduped
    .select(
        "id",
        "attr_key",
        "status_code_high_rank",
        "is_flagged_bucket",
        F.ntile(3).over(w4).alias("is_flagged_bucket_bucket"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

result = (
    scored
    .groupBy("attr_key")
    .agg(
        F.round(F.sum("is_flagged_bucket_bucket") / F.nullif(F.count(F.when(F.col("attr_key") == "region", 1)), F.lit(0)), 2).alias("is_flagged_bucket_bucket_per_region"),
        F.count(F.when(F.col("attr_key") == "version", 1)).alias("version_rows"),
    )
)
