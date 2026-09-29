from pyspark.sql import functions as F

result = (
    df_accounts
    .withColumn("balance_tier", F.when(F.col("balance") >= 59000, "HIGH").otherwise("LOW"))
    .withColumn("activity_month", F.date_trunc("MONTH", "opened_date"))
    .groupingSets(
        [["balance_tier", "status", "activity_month"], ["balance_tier", "status"], []],
        "balance_tier",
        "status",
        "activity_month",
    )
    .agg(
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.count_if(F.col("status") == "CLOSED").alias("closed_events"),
    )
    .orderBy(F.col("status").desc())
)
