from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)
w_hist = Window.partitionBy("account_type", "status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_type").orderBy(F.col("balance_avg_6").desc_nulls_last(), "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id")

enriched = (
    df_accounts
    .select(
        "*",
        F.avg("balance").over(w).alias("balance_avg_6"),
        F.first("status", ignorenulls=True).over(w_hist).alias("first_known_status"),
    )
)

ranked = (
    enriched
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

ranked_accounts = (
    ranked
    .withColumn("rn_2", F.row_number().over(w4))
    .filter(F.col("rn_2") == 1)
)

result = (
    ranked_accounts
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "rn",
        "rn_2",
    )
    .orderBy("customer_id")
)
