from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment").orderBy("signup_date", "id")

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
        "city",
        "signup_date",
        "is_active",
        "dedup_rank",
        F.when(F.col("country").rlike("^(TH|UK)$"), "OTHER_ZONE").when(F.col("country").rlike("^(US|SG)$"), "GROUP_A").otherwise("PRIORITY").alias("country_zone"),
    )
    .orderBy(F.col("dedup_rank").desc_nulls_last())
)
