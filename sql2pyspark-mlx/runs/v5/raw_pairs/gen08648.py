from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("city", "country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("age", "id")
w3 = Window.partitionBy("country").orderBy("signup_date", "id")
w4 = Window.partitionBy("segment", "city").orderBy(F.col("credit_limit").asc_nulls_first(), "id")

metrics = (
    df_customers
    .filter(F.col("credit_limit") < 9500)
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.min("age").over(w_ordered).alias("running_min_age"),
        F.count("age").over(w_ordered).alias("running_count_age"),
        F.sum("age").over(w_ordered).alias("running_sum_age"),
        F.max("age").over(w_ordered).alias("running_max_age"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

ranked_customers = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

kept = (
    ranked_customers
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.started_at") < F.col("s.signup_date")), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        "running_min_age",
        "running_count_age",
        "running_sum_age",
        F.when(F.col("city").rlike("^(New York|Berlin)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("city_zone"),
    )
    .orderBy("signup_date")
)
