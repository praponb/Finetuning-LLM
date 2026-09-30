from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment").orderBy(F.col("signup_date").desc(), "id")

latest = (
    df_customers
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.when(F.col("segment").rlike("^(SMB|Corporate)$"), "GROUP_B").when(F.col("segment").rlike("^(Consumer)$"), "CORE").otherwise("PRIORITY").alias("segment_zone"),
    )
)
