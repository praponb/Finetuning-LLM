from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "method")
w2 = Window.partitionBy("method").orderBy("paid_at_weekofyear", "id")
w3 = Window.partitionBy("status", "method").orderBy(F.col("prev_amount").desc(), "id")
w_grp = Window.partitionBy("status").orderBy("amount", "id")
w_ordered = Window.partitionBy("status")
w6 = Window.orderBy(F.col("status_max_amount").desc())
w7 = Window.orderBy("status_max_amount", "id")
w8 = Window.orderBy(F.col("next_status_max_amount").desc())
w9 = Window.partitionBy("method").orderBy(F.col("next_status_max_amount").desc(), "id")

cleaned = (
    df_payments
    .filter(F.col("order_id").isin([12, 24, 30]))
    .withColumn("paid_at_weekofyear", F.weekofyear("paid_at"))
)

enriched = (
    cleaned
    .select(
        "*",
        F.max("amount").over(w).alias("status_max_amount"),
        F.lag("amount", 2).over(w2).alias("prev_amount"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "method",
        "amount",
        "status",
        "status_max_amount",
        F.ntile(4).over(w3).alias("prev_amount_bucket"),
        F.lead("status_max_amount", 1).over(w_grp).alias("next_status_max_amount"),
        F.sum("amount").over(w_ordered).alias("status_sum_amount"),
    )
)

enriched_payments = (
    scored
    .select(
        "*",
        F.percent_rank().over(w6).alias("status_max_amount_percent_rank"),
        F.ntile(4).over(w7).alias("status_max_amount_bucket"),
        F.rank().over(w8).alias("next_status_max_amount_rank"),
    )
)

ranked = (
    enriched_payments
    .withColumn("latest_rank", F.row_number().over(w9))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "method",
        "amount",
        "status",
        "status_max_amount",
        "prev_amount_bucket",
        "next_status_max_amount",
        "status_sum_amount",
        "status_max_amount_bucket",
    )
)

result = (
    ranked
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_count"),
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_count_2"),
    )
    .filter(F.col("bank_transfer_count") > 1)
    .orderBy(F.col("status").desc_nulls_last())
)
