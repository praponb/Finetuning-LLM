from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("credit_limit").desc())
w2 = Window.partitionBy("element")
w3 = Window.partitionBy("element", "city").orderBy(F.col("credit_limit").desc_nulls_first())
w4 = Window.partitionBy("city").orderBy(F.col("element_count_credit_limit").asc_nulls_first())
w5 = Window.partitionBy("element").orderBy(F.col("credit_limit_cume_dist").desc())
w6 = Window.partitionBy("customer_id", "city").orderBy(F.col("credit_limit").desc_nulls_last())
w_ordered = Window.partitionBy("customer_id").orderBy(F.col("element_count_credit_limit_rank").asc_nulls_last())
w8 = Window.orderBy(F.col("credit_limit_percent_rank").desc_nulls_last())

flattened = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("element"))

joined = (
    flattened
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.credit_limit"), F.col("d.city"))
)

with_windows = (
    joined
    .withColumn("credit_limit_cume_dist", F.cume_dist().over(w))
    .withColumn("element_count_credit_limit", F.count("credit_limit").over(w2))
)

filtered = (
    with_windows
    .filter(F.col("city").isNotNull())
    .withColumn("credit_limit_cume_dist_minus_element_count_credit_limit", (F.col("credit_limit_cume_dist") - F.col("element_count_credit_limit")))
    .withColumn("resolved_element", F.coalesce("element", F.lit("NONE")))
)

enriched = (
    filtered
    .select(
        "customer_id",
        "credit_limit",
        "city",
        "credit_limit_cume_dist",
        "element_count_credit_limit",
        "credit_limit_cume_dist_minus_element_count_credit_limit",
        "resolved_element",
        F.rank().over(w3).alias("credit_limit_rank"),
        F.rank().over(w4).alias("element_count_credit_limit_rank"),
        F.percent_rank().over(w5).alias("credit_limit_cume_dist_percent_rank"),
        F.percent_rank().over(w6).alias("credit_limit_percent_rank"),
    )
)

metrics = (
    enriched
    .withColumn("element_count_credit_limit_rank_dense_rank", F.dense_rank().over(w_ordered))
    .withColumn("credit_limit_percent_rank_cume_dist", F.cume_dist().over(w8))
)

enriched_carts = (
    metrics
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select(
        "e.customer_id",
        "e.credit_limit",
        "e.city",
        "e.credit_limit_cume_dist",
        "e.element_count_credit_limit",
        "e.credit_limit_percent_rank",
        "e.element_count_credit_limit_rank_dense_rank",
        F.col("b.signup_date"),
        F.col("b.segment"),
        F.col("b.credit_limit").alias("customer_credit_limit"),
    )
)

result = (
    enriched_carts
    .select(
        "customer_id",
        "customer_credit_limit",
        "credit_limit_percent_rank",
        "credit_limit",
    )
    .unpivot(
        ids=["customer_id"],
        values=["customer_credit_limit", "credit_limit_percent_rank", "credit_limit"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
