from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("name").like("Pim%"))
    .select("signup_date", F.lit("customer").alias("source"))
    .union(
        df_accounts
        .filter(F.col("balance") > 78000)
        .select("opened_date", F.lit("account").alias("source"))
    )
)
