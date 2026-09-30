from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.to_date("txn_time") >= "2023-08-10") & ~F.col("is_flagged") & F.col("merchant").isin(["Starbucks", "Lazada"]))
    .select("txn_time")
)
