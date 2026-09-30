from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())

valid = (
    df_customers
    .filter((F.col("signup_date") > F.to_date(F.lit("2023-04-05"))) & F.col("city").isNotNull())
    .withColumn("segment_email_key", F.concat_ws("/", "segment", "email"))
)

result = (
    valid
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.email",
        "s.country",
        "s.signup_date",
        "s.segment_email_key",
        F.col("c.opened_date").alias("account_opened_date"),
        F.col("c.balance"),
    )
)
