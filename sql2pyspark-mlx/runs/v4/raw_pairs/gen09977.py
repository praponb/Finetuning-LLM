from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc())
w2 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.sum("amount").alias("sum_transaction_amount"))

metrics = (
    df_transactions
    .withColumn("amount_cume_dist", F.cume_dist().over(w))
    .withColumn("amount_max_7d", F.max("amount").over(w2))
)

enriched = (
    metrics
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.status"))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.account_type"), F.col("p.opened_date"))
)

result = (
    with_dims
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "amount_cume_dist",
        "amount_max_7d",
        "account_type",
        "opened_date",
        F.when(F.col("txn_type").rlike("^(CREDIT|FEE)$"), "OTHER_ZONE").otherwise("CORE").alias("txn_type_zone"),
        F.col("sum_transaction_amount"),
    )
    .orderBy(F.col("id").desc())
    .limit(20)
)
