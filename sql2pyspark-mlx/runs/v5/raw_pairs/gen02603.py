from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("city", "country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_customers
    .filter(F.col("segment").isin(["Consumer", "SMB", "Corporate"]))
    .withColumn("country_stddev_samp_credit_limit", F.stddev_samp("credit_limit").over(w))
    .withColumn("age_share", F.round((F.col("age") / F.sum("age").over(w)), 4))
    .withColumn("cumulative_credit_limit", F.sum("credit_limit").over(w2))
    .withColumn("cumulative_credit_limit_2", F.sum("credit_limit").over(w_part))
)

result = (
    metrics
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        "country_stddev_samp_credit_limit",
    )
    .orderBy(F.col("city").asc_nulls_last())
)
