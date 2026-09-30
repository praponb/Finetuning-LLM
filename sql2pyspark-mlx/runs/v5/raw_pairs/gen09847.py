from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment").orderBy("signup_date", "id")
w2 = Window.partitionBy("segment").orderBy(F.col("age").desc_nulls_last(), "id")
w3 = Window.partitionBy("segment", "city").orderBy("signup_date", "id")
w4 = Window.partitionBy("segment", "city")
w5 = Window.partitionBy("segment").orderBy(F.col("next_credit_limit").desc_nulls_last(), "id")
w6 = Window.partitionBy("activity_month").orderBy(F.col("smb_next_credit_limit").desc())

latest = (
    df_customers
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
    )
)

kept = (
    latest
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.start_date") > "2024-10-19"), "left_anti")
)

ranked = (
    kept
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    ranked
    .select(
        "id",
        "email",
        "segment",
        "signup_date",
        "age",
        F.lead("credit_limit").over(w3).alias("next_credit_limit"),
        F.lead("age").over(w3).alias("next_age"),
        F.row_number().over(w3).alias("row_seq"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w4)), 4).alias("credit_limit_share"),
    )
)

deduped = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .groupBy(
        F.date_trunc("MONTH", "signup_date").alias("activity_month"),
        F.when(F.col("age") >= 62, "HIGH").otherwise("LOW").alias("age_tier"),
    )
    .agg(
        F.sum("next_credit_limit").alias("total_next_credit_limit"),
        F.sum(F.when(F.col("segment") == "SMB", F.col("next_credit_limit")).otherwise(0)).alias("smb_next_credit_limit"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("segment") == "SMB", F.col("row_seq"))).alias("smb_total_row_seq"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_rows"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
    )
    .withColumn("smb_next_credit_limit_dense_rank", F.dense_rank().over(w6))
    .orderBy(F.col("total_next_credit_limit").desc())
)
