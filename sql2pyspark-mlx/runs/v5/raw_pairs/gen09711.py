from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") <= 3500) & ~F.col("is_flagged"))
    .groupBy("merchant")
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 5)
    .drop("row_count")
    .orderBy("merchant")
)
