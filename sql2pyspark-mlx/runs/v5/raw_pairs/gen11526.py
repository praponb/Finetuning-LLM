from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("credit_limit", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("k", "customer_id").orderBy(F.col("credit_limit").desc_nulls_last())
w3 = Window.partitionBy("k").orderBy("credit_limit", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("k").orderBy(F.col("credit_limit").desc(), "id", "k")
w5 = Window.partitionBy("customer_id").orderBy("credit_limit", "id", "k").rowsBetween(-3, Window.currentRow)
w6 = Window.orderBy("row_seq", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = df_carts.select("id", "customer_id", F.explode("attributes").alias("k", "value"))

with_dims = (
    lines
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.*", F.col("c.credit_limit"), F.col("c.age"))
)

metrics = (
    with_dims
    .select(
        "id",
        "customer_id",
        "k",
        "value",
        "credit_limit",
        "age",
        F.first("credit_limit", ignorenulls=True).over(w).alias("first_known_credit_limit"),
        F.rank().over(w2).alias("credit_limit_rank"),
        F.first("credit_limit", ignorenulls=True).over(w3).alias("first_known_credit_limit_2"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)

with_windows = (
    metrics
    .filter(F.col("row_seq").between(3, 7))
    .withColumn("last4_min_credit_limit", F.min("credit_limit").over(w5))
    .withColumn("last4_count_credit_limit", F.count("credit_limit").over(w5))
    .withColumn("last4_sum_credit_limit", F.sum("credit_limit").over(w5))
    .withColumn("last_known_first_known_credit_limit", F.last("first_known_credit_limit", ignorenulls=True).over(w6))
)

result = (
    with_windows
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.city").alias("city"), F.col("src.k").alias("k"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.age")).alias("max_age"),
    )
)
