from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc_nulls_first())
w2 = Window.partitionBy("segment").orderBy(F.col("signup_date").desc(), "id")

metrics = (
    df_customers
    .select(
        "*",
        F.cume_dist().over(w).alias("credit_limit_cume_dist"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

result = (
    metrics
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "credit_limit",
        "age",
        "credit_limit_cume_dist",
    )
    .orderBy(F.col("segment").desc())
)
