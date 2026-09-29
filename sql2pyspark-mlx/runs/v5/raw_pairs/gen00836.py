from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter(F.col("opened_date") > F.lit("2023-06-17").cast("date"))
    .select(
        "id",
        "customer_id",
        "opened_date",
        "status",
        F.round((F.col("balance") * 1.1), 2).alias("balance_scaled"),
    )
)

result = (
    valid
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.opened_date",
        "e.status",
        "e.balance_scaled",
        F.col("b.segment"),
    )
)
