from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), "id")
w2 = Window.partitionBy("city").orderBy(F.col("row_count").desc())

kept = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.customer_id").isin([12, 28])), "left_semi")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") < F.col("s.signup_date")), "left_anti")
)

base = (
    not_flagged
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-03-11"))) & (~F.col("city").eqNullSafe(F.lit("Tokyo"))))
    .select("*", F.trim("email").alias("clean_email"))
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    ranked
    .groupBy("city", F.date_trunc("year", "signup_date").alias("year_start"))
    .agg(
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.count("*").alias("row_count"),
        F.bit_or("age").alias("age_bit_or"),
    )
    .filter(F.col("row_count") > 1)
    .withColumn("row_count_percent_rank", F.percent_rank().over(w2))
    .orderBy(F.col("year_start").desc())
)
