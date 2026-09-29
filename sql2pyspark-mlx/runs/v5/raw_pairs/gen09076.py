from pyspark.sql import functions as F

result = (
    df_order_items
    .select("unit_price", F.lit("order_item").alias("record_type"))
    .unionByName(
        df_transactions
        .filter(F.col("account_id").isin([5, 37]))
        .select(
        F.col("amount").alias("unit_price"),
        F.lit("transaction").alias("record_type"),
    )
    )
    .orderBy("record_type")
)
