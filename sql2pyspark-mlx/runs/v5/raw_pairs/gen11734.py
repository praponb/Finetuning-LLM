from pyspark.sql import functions as F

base = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-04-29").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.when(F.col("status").isin(["CLOSED", "OPEN"]), "Group A").otherwise("Other").alias("status_group"),
        F.concat("status", F.lit(", "), "account_type").alias("status_account_type"),
    )
)

result = (
    base
    .groupBy("status", "customer_id")
    .pivot("account_type", ["LOAN", "CREDIT"])
    .agg(F.max("balance").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "status",
        "customer_id",
        F.col("LOAN_value_sum").alias("loan_value_sum"),
        F.col("LOAN_cnt").alias("loan_cnt"),
        F.col("CREDIT_value_sum").alias("credit_value_sum"),
        F.col("CREDIT_cnt").alias("credit_cnt"),
    )
)
