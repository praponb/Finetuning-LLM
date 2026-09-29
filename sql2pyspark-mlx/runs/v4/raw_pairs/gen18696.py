from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~(~F.col("is_active") & ((F.col("city") == "Chiang Mai") | (F.col("country") == "JP"))))
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("country") == "TH", F.col("age")).otherwise(0)).alias("th_age"),
    )
)
