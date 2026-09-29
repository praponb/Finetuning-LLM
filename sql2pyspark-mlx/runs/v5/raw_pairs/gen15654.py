from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")
w2 = Window.partitionBy("k").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k").orderBy("status_code")
w4 = Window.orderBy("status_code", "id", "k")
w5 = Window.partitionBy("k").orderBy("status_code_share", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("k").orderBy("status_code_cume_dist", "id", "k")
w7 = Window.partitionBy("k").orderBy(F.col("status_code_bucket").desc(), "id", "k")
w8 = Window.partitionBy("k").orderBy("latest_rank", "id", "k").rowsBetween(-3, -1)
w9 = Window.orderBy("status_code_cume_dist", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = df_app_logs.select("id", "status_code", F.explode("context").alias("k", "value"))

cleaned = (
    lines
    .filter(F.col("status_code") > 344)
    .withColumn("k_filled", F.coalesce("k", F.lit("N/A")))
    .withColumn("k_upper", F.upper("k"))
)

scored = (
    cleaned
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w)), 4))
    .withColumn("last_known_k", F.last("k", ignorenulls=True).over(w2))
    .withColumn("status_code_cume_dist", F.cume_dist().over(w3))
)

scored_app_logs = (
    scored
    .filter(F.col("status_code") > 280)
    .withColumn("k_sum_status_code", F.sum("status_code").over(w))
    .withColumn("cumulative_status_code_cume_dist", F.sum("status_code_cume_dist").over(w2))
    .withColumn("status_code_bucket", F.ntile(4).over(w4))
    .withColumn("first_known_k", F.first("k", ignorenulls=True).over(w5))
    .withColumn("status_code_cume_dist_bucket", F.ntile(10).over(w6))
)

deduped = (
    scored_app_logs
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "status_code",
        "k",
        "k_filled",
        "status_code_share",
        "last_known_k",
        "status_code_cume_dist",
        "k_sum_status_code",
        "cumulative_status_code_cume_dist",
        "status_code_bucket",
        "latest_rank",
        F.avg("status_code_cume_dist_bucket").over(w8).alias("status_code_cume_dist_bucket_avg_3_prior"),
        F.sum("cumulative_status_code_cume_dist").over(w9).alias("cumulative_cumulative_status_code_cume_dist"),
    )
)
