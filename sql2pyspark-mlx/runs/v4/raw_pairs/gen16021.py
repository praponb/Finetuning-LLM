from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("account_type")
w2 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_accounts
    .withColumn("account_type_max_balance", F.max("balance").over(w_hist))
    .withColumn("first_known_status", F.first("status", ignorenulls=True).over(w2))
)

result = (
    metrics
    .cube("customer_id", "account_type")
    .agg(
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_rows"),
        F.round(F.sum("account_type_max_balance") / F.nullif(F.count(F.when(F.col("status") == "CLOSED", 1)), F.lit(0)), 2).alias("account_type_max_balance_per_closed"),
        F.sum(F.when(F.col("account_type") == "SAVINGS", F.col("account_type_max_balance")).otherwise(0)).alias("savings_account_type_max_balance"),
        F.count("*").alias("row_count"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.count_if(F.col("account_type") == "CHECKING").alias("checking_events"),
    )
    .filter(F.col("closed_rows") > 1)
)
