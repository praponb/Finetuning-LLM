from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")
w2 = Window.partitionBy("city").orderBy(F.col("age").desc_nulls_first())
w3 = Window.orderBy("signup_date", "id")
w4 = Window.partitionBy("city", "segment")
w5 = Window.orderBy(F.col("signup_date").desc(), "id")

with_windows = (
    df_customers
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.percent_rank().over(w2).alias("age_percent_rank"),
        F.lead("credit_limit", 2, 0).over(w3).alias("next_credit_limit"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w4)), 4).alias("credit_limit_share"),
        F.row_number().over(w5).alias("event_seq_2"),
    )
)

result = (
    with_windows
    .groupBy("city")
    .agg(
        F.countDistinct("country").alias("distinct_countries"),
        F.count("*").alias("num_rows"),
        F.count_if(F.col("segment") == "SMB").alias("smb_events"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
    )
    .orderBy(F.col("city").desc_nulls_last())
)
