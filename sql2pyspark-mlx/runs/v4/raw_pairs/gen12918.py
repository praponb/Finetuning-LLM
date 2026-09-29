from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("segment")
w_recent = Window.partitionBy("segment", "city").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_customers
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 4))
    .withColumn("country_stddev_samp_credit_limit", F.stddev_samp("credit_limit").over(w))
)

valid = (
    scored
    .filter((F.col("signup_date") >= F.lit("2023-11-30").cast("date")) & F.col("segment").isNotNull() & F.col("city").isin(["Berlin", "London"]))
    .withColumn("country_title", F.initcap(F.trim("country")))
)

metrics = (
    valid
    .withColumn("segment_stddev_samp_credit_limit", F.stddev_samp("credit_limit").over(w2))
    .withColumn("running_age", F.sum("age").over(w_recent))
)

result = (
    metrics
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
    )
    .orderBy(F.col("city").desc_nulls_last())
)
