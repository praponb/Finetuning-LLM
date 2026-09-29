from pyspark.sql import functions as F

result = (
    df_accounts
    .select(F.lit("account").alias("kind"), "opened_date")
    .union(
        df_inventory
        .filter(F.col("quantity") > 179)
        .select(F.lit("inventory").alias("kind"), "last_restock")
    )
    .distinct()
)
