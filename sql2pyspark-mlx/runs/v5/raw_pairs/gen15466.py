from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("city").orderBy("signup_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("age", "id")

cleaned = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.date_trunc("MONTH", "signup_date").alias("signup_date_period"),
        F.concat("name", F.lit(", "), "segment").alias("name_segment"),
        F.coalesce("segment", F.lit("OTHER")).alias("resolved_segment"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "name",
        "country",
        "city",
        "age",
        F.sum("credit_limit").over(w_recent).alias("running_sum_credit_limit"),
        F.avg("credit_limit").over(w_recent).alias("running_avg_credit_limit"),
        F.min("credit_limit").over(w_recent).alias("running_min_credit_limit"),
        F.row_number().over(w2).alias("event_seq"),
    )
)
