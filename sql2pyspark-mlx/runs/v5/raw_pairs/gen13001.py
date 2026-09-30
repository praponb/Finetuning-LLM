from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id", "status").orderBy(F.col("status_length").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("status_length").desc_nulls_last())
w4 = Window.partitionBy("method").orderBy("paid_at", "id")

valid = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-08-10 05:00:00").cast("timestamp")) & F.col("order_id").isNotNull())
    .withColumn("amount_band", F.when(F.col("amount") >= 1660, "High").when(F.col("amount") >= 510, "Medium").otherwise("Low"))
    .withColumn("method_clean", F.coalesce("method", F.lit("NONE")))
)

filtered = (
    valid
    .filter(F.col("paid_at") >= F.to_timestamp(F.lit("2023-05-19 19:00:00")))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "amount_band",
        "method_clean",
        F.length("status").alias("status_length"),
        F.trim("status").alias("clean_status"),
    )
)

scored = (
    filtered
    .select(
        "id",
        "order_id",
        "method",
        "status_length",
        F.first("amount", ignorenulls=True).over(w).alias("first_known_amount"),
        F.row_number().over(w2).alias("seq_no"),
        F.dense_rank().over(w3).alias("status_length_dense_rank"),
        F.lag("status_length", 2).over(w4).alias("prev_status_length"),
    )
)

joined = (
    scored
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.method",
        "e.status_length",
        "e.first_known_amount",
        "e.seq_no",
        "e.status_length_dense_rank",
        "e.prev_status_length",
        F.col("p.discount"),
        F.col("p.amount"),
        F.col("p.status"),
    )
)

result = (
    joined
    .select("id", "status", "discount", "amount", "first_known_amount")
    .unpivot(
        ids=["id", "status"],
        values=["discount", "amount", "first_known_amount"],
        variableColumnName="metric",
        valueColumnName="val",
    )
)
