from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit")
w2 = Window.orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("credit_limit").desc(), "id")

filtered = (
    df_customers
    .filter(F.col("signup_date").between("2023-11-01", "2025-04-01"))
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
        F.substring("city", 1, 2).alias("city_prefix"),
        F.coalesce("city", F.lit("N/A")).alias("city_clean"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.cume_dist().over(w).alias("credit_limit_cume_dist"),
        F.sum("credit_limit").over(w2).alias("running_credit_limit"),
        F.row_number().over(w3).alias("seq_no"),
    )
)
