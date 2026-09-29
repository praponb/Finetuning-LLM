from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "account_type").orderBy(F.col("balance").desc())
w2 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("opened_date_month_end", "id")
w4 = Window.partitionBy("account_type", "status")
w5 = Window.partitionBy("account_type", "customer_id").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
    .withColumn("opened_date_minus_20d", F.date_sub("opened_date", 20))
    .withColumn("resolved_status", F.coalesce("status", F.lit("NONE")))
)

enriched = (
    cleaned
    .filter(F.month("opened_date_month_end").isin([2, 7, 12]))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "opened_date_month_end",
        F.cume_dist().over(w).alias("balance_cume_dist"),
        F.sum("balance").over(w2).alias("cumulative_balance"),
        F.row_number().over(w3).alias("event_seq"),
        F.stddev_samp("balance").over(w4).alias("account_type_stddev_samp_balance"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "cumulative_balance",
        "account_type_stddev_samp_balance",
        F.sum("balance").over(w5).alias("running_balance"),
        F.sum("balance").over(w6).alias("cumulative_balance_2"),
    )
)

cleaned_accounts = (
    metrics
    .filter(F.col("account_type").isin(["CREDIT", "CHECKING", "SAVINGS"]))
    .withColumn("running_balance_filled", F.coalesce("running_balance", F.lit(0)))
    .withColumn("adjusted_running_balance", F.round((F.col("running_balance") * 0.85), 2))
)

result = (
    cleaned_accounts
    .select(
        "id",
        "account_type",
        "balance",
        "cumulative_balance",
        "account_type_stddev_samp_balance",
        "running_balance",
        "cumulative_balance_2",
        F.when(F.col("account_type").rlike("^(LOAN|SAVINGS)$"), "GROUP_A").when(F.col("account_type").rlike("^(CHECKING|CREDIT)$"), "CORE").otherwise("EXTENDED").alias("account_type_zone"),
    )
)
