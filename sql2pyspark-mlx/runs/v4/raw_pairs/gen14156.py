from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id").orderBy("txn_time", "id")
w2 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy("amount_whole", "id")
w_ordered = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_recent = Window.orderBy("opened_date", "id")
w6 = Window.partitionBy("account_id")

base = (
    df_transactions
    .filter((F.col("txn_time") >= F.to_date(F.lit("2023-12-31"))) & F.col("merchant").isNotNull() & (F.col("account_id") == 18))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.floor("amount").alias("amount_whole"),
    )
)

enriched = (
    base
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("amount_count_30d", F.count("amount").over(w2))
    .withColumn("next_amount", F.lead("amount", 2).over(w3))
)

with_dims = (
    enriched
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.balance"), F.col("d.opened_date"))
)

metrics = (
    with_dims
    .select(
        "*",
        F.count("amount_whole").over(w_ordered).alias("amount_whole_count_7d"),
        F.lead("seq_no", 1).over(w_recent).alias("next_seq_no"),
        F.max("amount_whole").over(w_ordered).alias("amount_whole_max_7d"),
        F.stddev_samp("amount_count_30d").over(w6).alias("account_id_stddev_samp_amount_count_30d"),
    )
)

result = (
    metrics
    .select("id", "txn_type", "balance", "next_amount", "amount")
    .unpivot(
        ["id", "txn_type"],
        ["balance", "next_amount", "amount"],
        "metric_name",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
