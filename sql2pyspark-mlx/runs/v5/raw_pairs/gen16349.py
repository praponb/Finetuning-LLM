from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("method_length", "id")

base = (
    df_payments
    .filter(F.col("method").isin(["bank_transfer", "card"]))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.length("method").alias("method_length"),
    )
)

with_dims = (
    base
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.method",
        "s.amount",
        "s.status",
        "s.method_length",
        F.col("o.amount").alias("order_amount"),
        F.col("o.status").alias("order_status"),
        F.col("o.discount"),
    )
)

with_windows = (
    with_dims
    .select(
        "id",
        "status",
        "method_length",
        F.min("method_length").over(w).alias("running_min_method_length"),
        F.sum("method_length").over(w).alias("running_sum_method_length"),
        F.max("method_length").over(w).alias("running_max_method_length"),
        F.ntile(5).over(w2).alias("method_length_bucket"),
    )
)

filtered = (
    with_windows
    .filter(F.col("running_min_method_length") > 57)
    .select(
        "id",
        "method_length",
        "running_min_method_length",
        "running_sum_method_length",
        "running_max_method_length",
        "method_length_bucket",
        F.when(F.col("running_max_method_length") >= 87, "High").when(F.col("running_max_method_length") >= 20, "Medium").otherwise("Low").alias("running_max_method_length_level"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("method_length") >= 70, "PREMIUM").otherwise("STANDARD").alias("method_length_tier"),
        F.when(F.col("running_sum_method_length") >= 400, "PREMIUM").otherwise("STANDARD").alias("running_sum_method_length_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_first())
)
