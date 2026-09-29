from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "account_type").orderBy("opened_date", "id")
w2 = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id", "status").orderBy(F.col("next_balance").desc_nulls_first(), "id")
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("amount").alias("min_order_amount"))

with_windows = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "balance",
        "status",
        F.lead("balance", 1).over(w).alias("next_balance"),
        F.first("status", ignorenulls=True).over(w2).alias("first_known_status"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_type") == "DEBIT"), "left_anti")
)

filtered = (
    not_flagged
    .filter(F.col("balance").between(44000, 64000) & F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "balance",
        "status",
        "next_balance",
        "first_known_status",
        F.substring("status", 1, 3).alias("status_prefix"),
    )
)

with_dims = (
    filtered
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.status",
        "e.next_balance",
        "e.first_known_status",
        F.col("b.name"),
    )
)

latest = (
    with_dims
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "status",
        "first_known_status",
        "name",
        F.when(F.col("status").rlike("^(FROZEN|CLOSED)$"), "GROUP_A").when(F.col("status").rlike("^(OPEN)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("status_zone"),
        F.col("min_order_amount"),
    )
    .orderBy(F.col("status").desc(), "status_zone")
)
