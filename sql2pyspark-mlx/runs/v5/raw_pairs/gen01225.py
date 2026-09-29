from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(-3, -1)
w2 = Window.orderBy("opened_date", "id")
w3 = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_accounts
    .filter(F.col("customer_id").isNull() & (F.col("opened_date") >= F.lit("2023-04-02").cast("date")))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.col("balance").cast("int").alias("balance_int"),
        F.trim("account_type").alias("clean_account_type"),
        F.coalesce("status", F.lit("OTHER")).alias("status_filled"),
    )
)

result = (
    valid
    .filter(F.col("balance") >= 17000)
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        F.avg("balance_int").over(w).alias("balance_int_avg_3_prior"),
        F.lead("balance", 1).over(w2).alias("next_balance"),
        F.first("status", ignorenulls=True).over(w3).alias("first_known_status"),
    )
)
