from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("age", "id")
w3 = Window.partitionBy("city").orderBy(F.col("age").desc(), "id")

scored = (
    df_customers
    .withColumn("first_known_credit_limit", F.first("credit_limit", ignorenulls=True).over(w_recent))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("age_bucket", F.ntile(4).over(w3))
)

result = (
    scored
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("activity_month"))
    .agg(
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("city") == "Berlin", 1)), F.lit(0)), 2).alias("credit_limit_per_berlin"),
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_rows"),
        F.percentile_approx("age_bucket", [0.25, 0.5, 0.75]).alias("age_bucket_quartiles"),
    )
)
