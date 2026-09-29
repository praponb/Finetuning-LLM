from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

latest = (
    df_accounts
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_latest = (
    latest
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_time"),
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.merchant").isin(["Starbucks", "Lazada"])), "left_anti")
)

enriched = (
    not_flagged
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.city"))
)

result = (
    enriched
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "rank_in_group",
        "txn_time",
        "transaction_txn_type",
        "city",
        F.when(F.col("city").rlike("^(London|New York)$"), "GROUP_B").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "OTHER_ZONE").otherwise("CORE").alias("city_zone"),
    )
)
