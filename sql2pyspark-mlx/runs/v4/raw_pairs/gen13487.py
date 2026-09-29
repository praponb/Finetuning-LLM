from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .pivot("merchant", ["Lazada", "7-Eleven", "Grab"])
    .agg(F.sum("amount").alias("amount"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "account_id",
        F.col("Lazada_amount").alias("lazada_amount"),
        F.col("Lazada_item_count").alias("lazada_item_count"),
        F.col("7-Eleven_amount").alias("v_7_eleven_amount"),
        F.col("7-Eleven_item_count").alias("v_7_eleven_item_count"),
        F.col("Grab_amount").alias("grab_amount"),
        F.col("Grab_item_count").alias("grab_item_count"),
    )
)
