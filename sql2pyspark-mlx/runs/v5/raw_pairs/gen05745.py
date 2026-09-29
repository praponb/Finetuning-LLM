from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("balance").desc(), "id")
w3 = Window.partitionBy("account_type").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy(F.col("credit_limit").desc(), "id")
w5 = Window.orderBy(F.col("credit_limit").desc_nulls_first())
w6 = Window.partitionBy("country").orderBy("opened_date", "id").rowsBetween(-6, -1)

with_windows = (
    df_accounts
    .filter(F.col("status") == "CLOSED")
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        F.sum("balance").over(w).alias("cumulative_balance"),
        F.ntile(4).over(w2).alias("balance_bucket"),
    )
)

with_dims = (
    with_windows
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.account_type",
        "a.opened_date",
        "a.status",
        F.col("p.credit_limit"),
        F.col("p.name"),
    )
)

joined = (
    with_dims
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.*", F.col("c.country"), F.col("c.name").alias("customer_name"))
)

eligible = (
    joined
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") >= F.col("s.opened_date")), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "customer_id",
        "credit_limit",
        "name",
        "country",
        "customer_name",
        F.last("credit_limit", ignorenulls=True).over(w3).alias("last_known_credit_limit"),
        F.row_number().over(w4).alias("event_seq"),
        F.rank().over(w5).alias("credit_limit_rank"),
        F.avg("credit_limit").over(w6).alias("credit_limit_avg_6_prior"),
    )
)
