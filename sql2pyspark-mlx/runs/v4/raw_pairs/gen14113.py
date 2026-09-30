from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2023-10-01", "2024-10-01") & F.month("opened_date").isin([1, 6, 10]))
    .withColumn("store_name_prefix", F.substring("store_name", 1, 2))
    .withColumn("store_name_prefix_2", F.substring("store_name", 1, 4))
)
