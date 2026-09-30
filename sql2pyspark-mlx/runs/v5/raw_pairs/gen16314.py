from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("paid_at").asc_nulls_first())
w_grp = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("amount", "id")

ranked = (
    df_payments
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

base = (
    ranked
    .filter((F.col("paid_at") > F.lit("2023-10-01").cast("date")) & (F.col("method") != "promptpay"))
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        F.col("paid_at").cast("date").alias("paid_at_day"),
        F.coalesce("method", F.lit("OTHER")).alias("method_clean"),
    )
)

scored = (
    base
    .withColumn("running_count_amount", F.count("amount").over(w_grp))
    .withColumn("running_min_amount", F.min("amount").over(w_grp))
    .withColumn("running_sum_amount", F.sum("amount").over(w_grp))
    .withColumn("next_amount", F.lead("amount", 1).over(w3))
)

matched_keys = (
    scored
    .select("method", "id")
    .subtract(
        df_payments
        .filter(F.to_date("paid_at") >= "2024-07-31")
        .select("method", "id")
    )
)

verified = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.method") == F.col("k.method")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "running_count_amount",
        "running_min_amount",
        "running_sum_amount",
        "next_amount",
    )
    .orderBy(F.col("running_min_amount").desc(), "amount")
)
