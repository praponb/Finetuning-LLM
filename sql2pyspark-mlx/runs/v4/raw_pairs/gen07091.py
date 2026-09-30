from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

valid = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2024-02-13"))) & F.col("city").isNotNull())
    .select(
        "*",
        F.date_format("signup_date", "yyyy-MM").alias("signup_date_label"),
        F.concat_ws(",", "name", "country").alias("name_country_key"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.started_at").alias("session_started_at"),
        F.col("c.browser").alias("session_browser"),
    )
)
