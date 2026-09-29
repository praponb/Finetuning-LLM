from pyspark.sql import functions as F

base = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-04-06").cast("date")) & (F.col("opened_date") > "2023-04-01"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
)

result = (
    base
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.segment"), F.col("b.age"))
)
