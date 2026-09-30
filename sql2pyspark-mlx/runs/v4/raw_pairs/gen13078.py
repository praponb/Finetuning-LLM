from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id")
w2 = Window.partitionBy("status").orderBy("balance_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_type")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

filtered = (
    df_accounts
    .filter(F.col("customer_id") == 25)
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        F.round("balance", 0).alias("balance_rounded"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.lead("balance_rounded", 1).over(w).alias("next_balance_rounded"),
        F.last("balance_rounded", ignorenulls=True).over(w2).alias("last_known_balance_rounded"),
        F.avg("balance_rounded").over(w3).alias("account_type_avg_balance_rounded"),
    )
)

with_dims = (
    enriched
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.*", F.col("c.segment"), F.col("c.age"), F.col("c.email"))
)

result = (
    with_dims
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "status",
        "next_balance_rounded",
        "last_known_balance_rounded",
        "account_type_avg_balance_rounded",
        "age",
        F.col("sum_account_balance"),
    )
    .orderBy(F.col("last_known_balance_rounded").desc(), "account_type")
)
