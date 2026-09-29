from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("balance").desc_nulls_last())
w_recent = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("status").orderBy(F.col("opened_date").desc(), "id")
w_part = Window.partitionBy("customer_id").orderBy("balance", "id")
w6 = Window.partitionBy("city", "status").orderBy(F.col("signup_date").asc_nulls_last(), "id")

base = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-08-20").cast("date")) & F.month("opened_date").isin([2, 9, 12]))
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        F.trim("account_type").alias("clean_account_type"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("resolved_status"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.percent_rank().over(w).alias("balance_percent_rank"),
        F.sum("balance").over(w_recent).alias("running_balance"),
        F.count("balance").over(w3).alias("customer_id_count_balance"),
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

enriched = (
    metrics
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.city"), F.col("d.credit_limit"), F.col("d.signup_date"))
)

ranked = (
    enriched
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

totals = (
    ranked
    .cube(
        F.when(F.col("customer_id_count_balance") >= 16, "LARGE").otherwise("SMALL").alias("customer_id_count_balance_tier"),
        "city",
    )
    .agg(
        F.round(F.sum("balance_percent_rank") / F.nullif(F.count(F.when(F.col("status") == "CLOSED", 1)), F.lit(0)), 2).alias("balance_percent_rank_per_closed"),
        F.count_if(F.col("city") == "Tokyo").alias("tokyo_events"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance")).otherwise(0)).alias("frozen_balance"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("running_balance")).otherwise(0)).alias("bangkok_running_balance"),
        F.sum(F.when(F.col("city") == "New York", F.col("credit_limit")).otherwise(0)).alias("new_york_credit_limit"),
    )
)

result = (
    totals
    .select(
        "balance_percent_rank_per_closed",
        "cnt",
        "frozen_balance",
        "bangkok_running_balance",
        "new_york_credit_limit",
    )
    .orderBy(F.col("new_york_credit_limit").desc_nulls_first())
)
