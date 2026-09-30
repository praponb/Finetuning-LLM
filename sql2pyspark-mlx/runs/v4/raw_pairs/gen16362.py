from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") == "FROZEN") & F.col("customer_id").isin([5, 14, 30, 32]))
    .select(
        "id",
        "opened_date",
        F.round("balance", 2).alias("balance_rounded"),
        F.when(F.col("balance") >= 73000, "High").when(F.col("balance") >= 6000, "Medium").otherwise("Low").alias("balance_band"),
    )
)
