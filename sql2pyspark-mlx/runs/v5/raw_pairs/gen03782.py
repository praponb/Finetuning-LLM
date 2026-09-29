from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

not_flagged = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date")), "left_anti")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") <= F.col("s.opened_date")), "left_anti")
)

with_dims = (
    kept
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.balance",
        "e.opened_date",
        F.col("p.email"),
        F.col("p.name"),
        F.col("p.country"),
    )
)

scored = (
    with_dims
    .withColumn("running_max_balance", F.max("balance").over(w))
    .withColumn("running_avg_balance", F.avg("balance").over(w))
    .withColumn("running_count_balance", F.count("balance").over(w))
)

eligible = (
    scored
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") <= F.col("s.opened_date")), "left_semi")
)

with_last = (
    eligible
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.balance",
        "s.name",
        "s.country",
        "s.running_max_balance",
        F.col("c.amount"),
    )
)

result = (
    with_last
    .select("id", "country", "amount", "running_max_balance")
    .unpivot(
        ids=["id", "country"],
        values=["amount", "running_max_balance"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
)
