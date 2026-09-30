from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount_rounded").desc_nulls_first(), "id")

filtered = (
    df_payments
    .filter((F.col("paid_at") >= F.to_date(F.lit("2023-06-30"))) & F.col("method").isNull())
    .withColumn("amount_rounded", F.round("amount", 2))
    .withColumn("status_upper", F.upper("status"))
)

deduped = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

filtered_payments = (
    deduped
    .filter(F.col("paid_at") > F.lit("2023-10-04 07:00:00").cast("timestamp"))
    .withColumn("method_prefix", F.substring("method", 1, 3))
)

base = (
    filtered_payments
    .filter(F.col("paid_at") > F.lit("2023-09-03 17:00:00").cast("timestamp"))
    .select(
        "*",
        (F.col("latest_rank") - F.col("amount")).alias("latest_rank_minus_amount"),
    )
)

result = (
    base
    .groupBy("order_id", "status")
    .pivot("method", ["promptpay", "cash"])
    .agg(
        F.max("latest_rank_minus_amount").alias("value_sum"),
        F.count(F.lit(1)).alias("n"),
    )
)
