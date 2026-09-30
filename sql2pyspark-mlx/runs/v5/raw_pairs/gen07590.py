from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("segment").orderBy("signup_date_minus_87d", "id")
w3 = Window.partitionBy("segment", "status").orderBy(F.col("age").asc_nulls_last())
w4 = Window.partitionBy("segment")
w_grp = Window.partitionBy("status").orderBy("signup_date_minus_87d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest_match = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.segment",
        "s.signup_date",
        "s.credit_limit",
        "s.age",
        F.col("c.status"),
    )
)

base = (
    latest_match
    .filter(F.col("segment").isNotNull())
    .select(
        "*",
        F.last_day("signup_date").alias("signup_date_month_end"),
        F.date_sub("signup_date", 87).alias("signup_date_minus_87d"),
        F.coalesce("status", F.lit("DIRECT")).alias("status_filled"),
    )
)

scored = (
    base
    .filter(F.col("signup_date_minus_87d").between("2023-04-01", "2025-01-05"))
    .select(
        "id",
        "credit_limit",
        "age",
        "status_filled",
        F.row_number().over(w2).alias("seq_no"),
        F.cume_dist().over(w3).alias("age_cume_dist"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w4)), 4).alias("credit_limit_share"),
        F.first("age", ignorenulls=True).over(w_grp).alias("first_known_age"),
    )
)

valid = (
    scored
    .filter(F.col("age") > 35)
    .withColumn("age_cume_dist_times_credit_limit_share", (F.col("age_cume_dist") * F.col("credit_limit_share")))
)

result = (
    valid
    .groupBy("seq_no")
    .agg(
        F.corr("credit_limit", "age").alias("corr_credit_limit_age"),
        F.bit_and("age").alias("age_bit_and"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("age_bit_and").asc_nulls_first())
)
