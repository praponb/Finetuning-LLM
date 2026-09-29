from pyspark.sql import functions as F

result = (
    df_products
    .select(F.lit("product").alias("origin"), "launch_date")
    .union(
        df_accounts
        .filter(F.col("account_type").isin(["CHECKING", "CREDIT"]))
        .select(F.lit("account").alias("origin"), "opened_date")
    )
)
