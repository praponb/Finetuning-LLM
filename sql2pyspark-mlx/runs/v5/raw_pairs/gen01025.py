from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id")
w2 = Window.partitionBy("status", "order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w_hist = Window.partitionBy("order_status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w_ordered = Window.partitionBy("order_status")

scored = (
    df_payments
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("first_known_method", F.first("method", ignorenulls=True).over(w2))
    .withColumn("amount_count_30d", F.count("amount").over(w3))
)

kept_keys = (
    scored
    .select("method", "id")
    .intersect(
        df_payments
        .filter(F.col("status").isin(["REFUNDED", "SUCCESS", "FAILED"]))
        .select("method", "id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.method") == F.col("k.method")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter((F.col("paid_at") >= F.lit("2023-06-11").cast("date")) & (~F.col("method").eqNullSafe(F.lit("card"))))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "seq_no",
        "first_known_method",
        "amount_count_30d",
        F.round("amount", 2).alias("amount_rounded"),
    )
)

joined = (
    valid
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id").eqNullSafe(F.col("o.id")), "inner")
    .select("src.*", F.col("o.amount").alias("order_amount"))
)

with_dims = (
    joined
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.store_id").isNull())
    .select("e.*", F.col("d.status").alias("order_status"))
)

valid_payments = (
    with_dims
    .filter(F.col("amount") > 690)
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        "seq_no",
        "first_known_method",
        "amount_count_30d",
        "amount_rounded",
        "order_amount",
        "order_status",
        F.round((F.col("amount") * 100), 2).alias("amount_adjusted"),
    )
)

metrics = (
    valid_payments
    .withColumn("amount_adjusted_avg_30d", F.avg("amount_adjusted").over(w_hist))
    .withColumn("order_status_sum_amount_rounded", F.sum("amount_rounded").over(w_ordered))
    .withColumn("amount_adjusted_sum_30d", F.sum("amount_adjusted").over(w_hist))
)

result = (
    metrics
    .select("order_id", "amount_adjusted", "order_amount", "amount_rounded")
    .unpivot(
        ids=["order_id"],
        values=["amount_adjusted", "order_amount", "amount_rounded"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
