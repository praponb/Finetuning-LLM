from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "status",
        F.concat("account_type", F.lit(" "), "status").alias("account_type_status"),
    )
    .orderBy(F.col("id").desc())
)
