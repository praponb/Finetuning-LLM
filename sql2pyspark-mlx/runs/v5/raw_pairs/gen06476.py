from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id")

ranked = (
    df_accounts
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    ranked
    .withColumn("week_start", F.date_trunc("WEEK", "opened_date"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "opened_date"))
    .withColumn("balance_tier", F.when(F.col("balance") >= 64000, "HIGH").otherwise("LOW"))
    .cube("week_start", "quarter_start", "balance_tier")
    .agg(
        F.avg(F.when(F.col("status") == "CLOSED", F.col("balance"))).alias("closed_avg_balance"),
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_count"),
        F.grouping("week_start").alias("grouping_id"),
    )
    .filter(F.col("closed_avg_balance") > 0)
)
