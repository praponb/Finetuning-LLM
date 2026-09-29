from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") <= 2900) & ~F.col("is_flagged") & F.col("merchant").isin(["Grab", "Amazon"]))
)
