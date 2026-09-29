from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.orderBy("age")
w3 = Window.orderBy(F.col("age_dense_rank").desc())
w4 = Window.orderBy(F.col("corr_credit_limit_age").desc_nulls_first())
w5 = Window.orderBy("age_dense_rank")

matched_keys = (
    df_customers
    .select("city", "country")
    .subtract(
        df_customers
        .filter(F.col("segment") == "SMB")
        .select("city", "country")
    )
)

kept_rows = (
    df_customers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .select(
        "id",
        "name",
        "email",
        "credit_limit",
        "age",
        F.corr("credit_limit", "age").over(w).alias("corr_credit_limit_age"),
        F.dense_rank().over(w2).alias("age_dense_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "name",
        "email",
        "age_dense_rank",
        F.dense_rank().over(w3).alias("age_dense_rank_dense_rank"),
        F.percent_rank().over(w4).alias("corr_credit_limit_age_percent_rank"),
        F.rank().over(w5).alias("age_dense_rank_rank"),
    )
)

kept = (
    metrics
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.status").isin(["CANCELLED", "PENDING"])), "left_anti")
)

kept_keys = (
    kept
    .select("id")
    .subtract(
        df_customers
        .filter(F.col("segment").isNull())
        .select("id")
    )
)

reconciled = (
    kept
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "name",
        "age_dense_rank",
        "age_dense_rank_dense_rank",
        "age_dense_rank_rank",
        F.when(F.col("name").rlike("^(Mia Rossi|Kanya Srisuk)$"), "CORE").otherwise("OTHER_ZONE").alias("name_zone"),
    )
    .orderBy(F.col("age_dense_rank_rank").asc_nulls_last())
)
