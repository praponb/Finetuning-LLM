from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("signup_date").desc_nulls_first(), "id")

ranked = (
    df_customers
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .select(
        "id",
        "email",
        "country",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "row_num",
        F.when(F.col("country").rlike("^(TH|DE)$"), "PRIORITY").otherwise("CORE").alias("country_zone"),
    )
    .orderBy(F.col("country").desc_nulls_first())
)
