from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("order_id")

base = (
    df_payments
    .filter(F.col("paid_at") >= F.to_timestamp(F.lit("2023-07-05 09:00:00")))
    .withColumn("method_upper", F.upper("method"))
)

with_windows = (
    base
    .withColumn("amount_bucket", F.ntile(5).over(w))
    .withColumn("amount_avg_5_prior", F.avg("amount").over(w2))
    .withColumn("order_id_max_amount", F.max("amount").over(w3))
)

kept_keys = (
    with_windows
    .select("id", "status")
    .subtract(
        df_payments
        .filter(F.col("status").isin(["FAILED", "REFUNDED", "SUCCESS"]))
        .select("id", "status")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("amount_avg_5_prior") >= 1539, "HIGH").otherwise("LOW").alias("amount_avg_5_prior_tier"),
    )
    .agg(
        F.avg(F.when(F.col("status") == "REFUNDED", F.col("amount_avg_5_prior"))).alias("refunded_avg_amount_avg_5_prior"),
        F.percentile_approx("order_id_max_amount", [0.25, 0.5, 0.75]).alias("order_id_max_amount_quartiles"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.max(F.when(F.col("status") == "SUCCESS", F.col("amount_bucket"))).alias("success_max_amount_bucket"),
    )
)
