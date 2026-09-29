from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") > "2023-04-01")
    .select(F.col("opened_date").alias("event_date"), F.lit("account").alias("source"))
    .unionByName(
        df_store_sales
        .select(F.col("sale_date").alias("event_date"), F.lit("store_sale").alias("source"))
    )
    .distinct()
    .orderBy("event_date")
)
