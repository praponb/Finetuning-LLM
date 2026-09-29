from pyspark.sql import functions as F

result = (
    df_transactions
    .select("amount", F.lit("transaction").alias("source"))
    .union(
        df_order_items
        .select("unit_price", F.lit("order_item").alias("source"))
    )
    .distinct()
    .orderBy("source")
)
