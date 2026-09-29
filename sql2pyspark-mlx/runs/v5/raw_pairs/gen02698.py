from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "CHECKING"]))
    .select(F.lit("account").alias("origin"), F.col("opened_date").alias("event_date"))
    .unionByName(
        df_products
        .select(F.lit("product").alias("origin"), F.col("launch_date").alias("event_date"))
    )
    .orderBy("event_date")
)
