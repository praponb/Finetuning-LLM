from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("status_code")
w2 = Window.orderBy("status_code", "id", "attr_key").rowsBetween(-2, -1)
w3 = Window.partitionBy("host").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("status_code_dense_rank", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

deduped = (
    exploded
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched = (
    deduped
    .withColumn("status_code_avg_2_prior", F.avg("status_code").over(w2))
    .withColumn("last_known_attr_key", F.last("attr_key", ignorenulls=True).over(w3))
    .withColumn("first_known_attr_key", F.first("attr_key", ignorenulls=True).over(w_grp))
    .withColumn("status_code_dense_rank", F.dense_rank().over(w))
)

metrics = (
    enriched
    .select(
        "*",
        F.first("status_code_dense_rank", ignorenulls=True).over(w5).alias("first_known_status_code_dense_rank"),
        F.min("status_code").over(w6).alias("attr_key_min_status_code"),
    )
)

result = (
    metrics
    .groupBy("attr_key")
    .agg(
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_rows"),
        F.max("attr_key_min_status_code").alias("max_attr_key_min_status_code"),
        F.round(F.sum("status_code") / F.nullif(F.count(F.when(F.col("attr_key") == "region", 1)), F.lit(0)), 2).alias("status_code_per_region"),
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
    )
)
