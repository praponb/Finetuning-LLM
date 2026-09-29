from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment").orderBy(F.col("credit_limit").desc())
w2 = Window.partitionBy("country").orderBy(F.col("credit_limit").desc_nulls_first())
w3 = Window.partitionBy("city").orderBy(F.col("credit_limit").desc_nulls_first())
w4 = Window.partitionBy("city").orderBy(F.col("credit_limit_percent_rank").asc_nulls_first())
w5 = Window.partitionBy("segment")
w6 = Window.partitionBy("city")
w7 = Window.partitionBy("segment").orderBy(F.col("age").desc_nulls_last())

with_windows = (
    df_customers
    .select(
        "*",
        F.dense_rank().over(w).alias("credit_limit_dense_rank"),
        F.dense_rank().over(w2).alias("credit_limit_dense_rank_2"),
        F.percent_rank().over(w3).alias("credit_limit_percent_rank"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left_anti")
)

matched_keys = (
    eligible
    .select("id", "country")
    .subtract(
        df_customers
        .filter(F.col("signup_date").between("2023-09-19", "2023-12-01"))
        .select("id", "country")
    )
)

kept_rows = (
    eligible
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("credit_limit_percent_rank_dense_rank", F.dense_rank().over(w4))
    .withColumn("age_share", F.round((F.col("age") / F.sum("age").over(w5)), 4))
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w5)), 4))
    .withColumn("age_share_2", F.round((F.col("age") / F.sum("age").over(w6)), 4))
    .withColumn("age_rank", F.rank().over(w7))
)
