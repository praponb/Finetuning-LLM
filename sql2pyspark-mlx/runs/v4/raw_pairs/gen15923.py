from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("age").asc_nulls_first())
w2 = Window.orderBy(F.col("age").desc())
w_grp = Window.partitionBy("segment", "country")
w4 = Window.orderBy(F.col("credit_limit").asc_nulls_first())

matched_keys = (
    df_customers
    .select("city", "segment")
    .subtract(
        df_customers
        .filter(F.month("signup_date").isin([3, 9, 10]))
        .select("city", "segment")
    )
)

kept_rows = (
    df_customers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .withColumn("age_dense_rank", F.dense_rank().over(w))
    .withColumn("age_cume_dist", F.cume_dist().over(w2))
    .withColumn("segment_count_age", F.count("age").over(w_grp))
    .withColumn("credit_limit_cume_dist", F.cume_dist().over(w4))
)

kept = (
    enriched
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.started_at") <= F.col("s.signup_date")), "left_anti")
)

remaining_keys = (
    kept
    .select("id", "segment")
    .subtract(
        df_customers
        .filter(F.col("email").isNull())
        .select("id", "segment")
    )
)

reconciled = (
    kept
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

daily = (
    reconciled
    .groupBy(
        F.when(F.col("credit_limit") >= 15400, "LARGE").otherwise("SMALL").alias("credit_limit_tier"),
    )
    .agg(
        F.bit_and("age").alias("age_bit_and"),
        F.sum("credit_limit_cume_dist").alias("total_credit_limit_cume_dist"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_rows"),
    )
)

result = daily.select("credit_limit_tier", "total_credit_limit_cume_dist")
