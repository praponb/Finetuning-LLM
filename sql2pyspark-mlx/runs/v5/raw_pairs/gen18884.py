from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("account_type")
w3 = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("account_type").orderBy("balance", "id")
w_hist = Window.partitionBy("account_type").orderBy("prev_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("account_type").orderBy("balance_avg_5_prior", "id").rowsBetween(-5, -1)
w7 = Window.orderBy("balance_avg_5_prior", "id")
w8 = Window.partitionBy("account_type").orderBy(F.col("prev_balance").desc_nulls_first())
w9 = Window.partitionBy("account_type").orderBy("prev_balance")

ranked = (
    df_accounts
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "account_type", "balance", "opened_date")
)

metrics = (
    ranked
    .select(
        "id",
        "account_type",
        "balance",
        F.sum("balance").over(w2).alias("account_type_sum_balance"),
        F.avg("balance").over(w3).alias("balance_avg_5_prior"),
        F.lag("balance", 1, 0).over(w4).alias("prev_balance"),
        F.ntile(4).over(w4).alias("balance_bucket"),
    )
)

metrics_accounts = (
    metrics
    .withColumn("running_sum_account_type_sum_balance", F.sum("account_type_sum_balance").over(w_hist))
    .withColumn("running_min_account_type_sum_balance", F.min("account_type_sum_balance").over(w_hist))
    .withColumn("running_max_account_type_sum_balance", F.max("account_type_sum_balance").over(w_hist))
    .withColumn("balance_avg_5_prior_avg_5_prior", F.avg("balance_avg_5_prior").over(w6))
)

metrics_accounts_2 = (
    metrics_accounts
    .select(
        "id",
        "account_type",
        "balance",
        "prev_balance",
        "running_sum_account_type_sum_balance",
        "running_min_account_type_sum_balance",
        "balance_avg_5_prior_avg_5_prior",
        F.min("account_type_sum_balance").over(w2).alias("account_type_min_account_type_sum_balance"),
        F.lag("account_type_sum_balance", 1).over(w7).alias("prev_account_type_sum_balance"),
        F.rank().over(w8).alias("prev_balance_rank"),
    )
)

filtered = (
    metrics_accounts_2
    .filter(F.col("prev_account_type_sum_balance") < 208467)
    .select(
        "id",
        "account_type",
        "balance",
        "prev_balance",
        "running_sum_account_type_sum_balance",
        "running_min_account_type_sum_balance",
        "balance_avg_5_prior_avg_5_prior",
        "account_type_min_account_type_sum_balance",
        F.round("running_min_account_type_sum_balance", 0).alias("running_min_account_type_sum_balance_rounded"),
        F.lower("account_type").alias("account_type_lower"),
    )
)

latest = (
    filtered
    .withColumn("dedup_rank", F.rank().over(w9))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "account_type",
        F.col("balance").alias("BALANCE_VAL"),
        F.col("running_min_account_type_sum_balance").alias("RUNNING_MIN_ACCOUNT_TYPE_SUM_BALANCE"),
        F.col("running_sum_account_type_sum_balance").alias("RUNNING_SUM_ACCOUNT_TYPE_SUM_BALANCE_VAL"),
    )
    .unpivot(
        ids=["id", "account_type"],
        values=["BALANCE_VAL", "RUNNING_MIN_ACCOUNT_TYPE_SUM_BALANCE", "RUNNING_SUM_ACCOUNT_TYPE_SUM_BALANCE_VAL"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
