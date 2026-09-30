from pyspark.sql import functions as F

filtered = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2023-04-08").cast("date"))
    .select(
        "*",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)

result = (
    filtered
    .select("id", "segment")
    .intersectAll(
        df_customers
        .filter(F.col("email").like("%@company.co"))
        .select("id", "segment")
    )
)
