from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") != "FROZEN") & F.col("account_type").isin(["LOAN", "CREDIT"]) & F.col("balance").between(38000, 55000))
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "LOAN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_loan"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.max("opened_date").alias("latest_opened_date"),
    )
    .orderBy(F.col("pct_loan").desc(), F.col("status").desc())
)
