from pyspark.sql import functions as F

valid = (
    df_customers
    .filter((F.col("credit_limit") < 12500) & (~F.col("city").eqNullSafe(F.lit("Berlin"))))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)

result = (
    valid
    .groupBy("segment", "country")
    .pivot("city", ["Chiang Mai", "Singapore", "London"])
    .agg(F.avg("age").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "segment",
        "country",
        F.col("Chiang Mai_amount").alias("chiang_mai_amount"),
        F.col("Chiang Mai_n").alias("chiang_mai_n"),
        F.col("Singapore_amount").alias("singapore_amount"),
        F.col("Singapore_n").alias("singapore_n"),
        F.col("London_amount").alias("london_amount"),
        F.col("London_n").alias("london_n"),
    )
)
