from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        "account_type",
        F.when(F.col("account_type").isin(["SAVINGS", "CHECKING"]), "priority").otherwise("other").alias("account_type_group"),
    )
    .agg(F.min("opened_date").alias("min_opened_date"))
    .orderBy(F.col("min_opened_date").desc())
)
