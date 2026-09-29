from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "balance",
        "customer_id",
        F.date_add("opened_date", 18).alias("opened_date_plus_18d"),
    )
    .orderBy(F.col("opened_date_plus_18d").desc(), F.col("balance").desc())
)
