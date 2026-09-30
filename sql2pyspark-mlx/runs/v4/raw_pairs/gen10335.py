from pyspark.sql import functions as F

result = (
    df_accounts
    .select("opened_date")
    .unionByName(
        df_orders
        .filter(F.col("amount").between(680, 1570))
        .select(F.col("order_date").alias("opened_date"))
    )
    .distinct()
)
