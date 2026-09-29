from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("city").orderBy("age", "id")
w2 = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc_nulls_last(), "id")
w_ordered = Window.orderBy(F.col("row_seq").desc(), "id")
w4 = Window.orderBy("row_seq", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.orderBy(F.col("next_age").desc(), "id")
w_running = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("event_seq").desc(), "id")

filtered = (
    df_customers
    .filter(F.col("segment").isNotNull() & (F.col("credit_limit") >= 15700))
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "credit_limit",
        "age",
        F.trim("email").alias("clean_email"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

with_windows = (
    filtered
    .filter(F.col("age") > 62)
    .select(
        "id",
        "segment",
        "credit_limit",
        "age",
        "clean_email",
        "city_clean",
        F.lag("age").over(w_part).alias("prev_age"),
        F.lead("age").over(w_part).alias("next_age"),
        F.lag("credit_limit").over(w_part).alias("prev_credit_limit"),
        F.row_number().over(w_part).alias("row_seq"),
    )
)

ranked = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "city_clean", "prev_age", "next_age", "row_seq")
)

scored = (
    ranked
    .select(
        "id",
        "city_clean",
        "prev_age",
        "row_seq",
        F.ntile(10).over(w_ordered).alias("row_seq_bucket"),
        F.avg("prev_age").over(w4).alias("prev_age_avg_5"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

scored_customers = (
    scored
    .withColumn("running_count_row_seq", F.count("row_seq").over(w_running))
    .withColumn("running_avg_row_seq", F.avg("row_seq").over(w_running))
    .withColumn("running_max_row_seq", F.max("row_seq").over(w_running))
    .withColumn("event_seq_2", F.row_number().over(w7))
)

result = (
    scored_customers
    .select(
        "id",
        "city_clean",
        "prev_age",
        "row_seq",
        "row_seq_bucket",
        "prev_age_avg_5",
        "event_seq",
        "running_avg_row_seq",
        "event_seq_2",
    )
    .orderBy(F.col("row_seq").desc_nulls_first(), "prev_age_avg_5")
)
