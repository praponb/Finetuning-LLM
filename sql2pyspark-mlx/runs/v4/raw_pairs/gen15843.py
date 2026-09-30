from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment").orderBy("credit_limit", "id")

not_flagged = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") < F.col("s.signup_date")), "left_semi")
)

latest = (
    not_flagged
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

kept = (
    latest
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") < F.col("s.signup_date")), "left_semi")
)

result = (
    kept
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "is_active",
        "credit_limit",
        "age",
        "latest_rank",
    )
    .orderBy(F.col("age").desc_nulls_last())
)
