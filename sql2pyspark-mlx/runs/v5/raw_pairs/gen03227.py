from pyspark.sql import functions as F

result = (
    df_accounts
    .select("opened_date", F.lit("account").alias("origin"))
    .union(
        df_inventory
        .filter(F.col("quantity").between(199, 220))
        .select("last_restock", F.lit("inventory").alias("origin"))
    )
)
