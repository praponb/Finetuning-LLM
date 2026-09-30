from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc())
w2 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_type").orderBy(F.col("balance").desc_nulls_last())

scored = (
    df_accounts
    .filter(F.col("account_type") == "SAVINGS")
    .select(
        "*",
        F.dense_rank().over(w).alias("balance_dense_rank"),
        F.first("status", ignorenulls=True).over(w2).alias("first_known_status"),
        F.dense_rank().over(w3).alias("balance_dense_rank_2"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("balance") >= 59000, "HIGH").otherwise("LOW").alias("balance_tier"),
        F.date_trunc("MONTH", "opened_date").alias("activity_month"),
        "account_type",
    )
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.round(F.avg("balance_dense_rank_2"), 2).alias("avg_balance_dense_rank_2"),
        F.round(F.avg("balance_dense_rank_2"), 2).alias("avg_balance_dense_rank_2_2"),
        F.round(F.sum("balance_dense_rank") / F.nullif(F.count(F.when(F.col("account_type") == "CHECKING", 1)), F.lit(0)), 2).alias("balance_dense_rank_per_checking"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("balance_dense_rank")).otherwise(0)).alias("checking_balance_dense_rank"),
    )
    .orderBy("avg_balance_dense_rank_2_2")
)
