from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc(), "id")

filtered = (
    df_customers
    .filter((~F.col("segment").eqNullSafe(F.lit("Consumer"))) & F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "city",
        "is_active",
        "credit_limit",
        "age",
        F.upper(F.trim("email")).alias("email_normalized"),
        F.coalesce("segment", F.lit("NONE")).alias("resolved_segment"),
    )
)

result = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "email", "city", "credit_limit", "resolved_segment")
)
