from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").asc_nulls_first(), "id")

deduped = (
    df_customers
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "dedup_rank",
    )
    .orderBy("segment")
)
