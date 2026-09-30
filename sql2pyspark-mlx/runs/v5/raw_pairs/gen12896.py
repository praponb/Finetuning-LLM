from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active") & (F.col("country").isin(["SG", "UK"]) | F.col("segment").isin(["Corporate", "Consumer"])))
    .select(
        "id",
        "name",
        "city",
        (F.col("credit_limit") * F.col("age")).alias("credit_limit_times_age"),
    )
    .orderBy("id", F.col("name").desc())
)
