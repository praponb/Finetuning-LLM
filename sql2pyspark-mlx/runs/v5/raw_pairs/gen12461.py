from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

enriched = (
    df_sessions
    .filter(F.col("customer_id") == 11)
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.last("country", ignorenulls=True).over(w).alias("last_known_country"),
    )
)

result = (
    enriched
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "last_known_country",
        F.col("sum_account_balance"),
    )
)
