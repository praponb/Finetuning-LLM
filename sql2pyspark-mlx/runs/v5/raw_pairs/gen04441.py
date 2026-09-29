from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "account_type").orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        F.min("balance").over(w).alias("last3_min_balance"),
        F.count("balance").over(w).alias("last3_count_balance"),
        F.avg("balance").over(w).alias("last3_avg_balance"),
    )
)
