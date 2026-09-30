from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(F.lit("store_sale").alias("record_type"), "sale_date")
    .union(
        df_accounts
        .filter(F.col("account_type").isin(["CHECKING", "LOAN", "CREDIT"]))
        .select(F.lit("account").alias("record_type"), "opened_date")
    )
)
