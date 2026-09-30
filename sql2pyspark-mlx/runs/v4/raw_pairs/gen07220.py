from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .select("opened_date")
    .unionByName(
        df_products
        .filter(F.array_contains("tags", "premium"))
        .select(F.col("launch_date").alias("opened_date"))
    )
    .orderBy("opened_date")
)
