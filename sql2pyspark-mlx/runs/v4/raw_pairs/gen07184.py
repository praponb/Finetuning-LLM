from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "opened_date",
        F.weekofyear("opened_date").alias("opened_date_weekofyear"),
    )
    .orderBy(F.col("opened_date_weekofyear").desc())
)
