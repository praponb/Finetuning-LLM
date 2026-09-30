from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("city").orderBy("credit_limit", "id")

metrics = (
    df_customers
    .withColumn("next_credit_limit", F.lead("credit_limit").over(w_grp))
    .withColumn("next_age", F.lead("age").over(w_grp))
    .withColumn("row_seq", F.row_number().over(w_grp))
)

result = (
    metrics
    .select(
        "id",
        "name",
        "email",
        "country",
        "credit_limit",
        "age",
        "next_credit_limit",
        "next_age",
        F.when(F.col("name").rlike("^(Jack Taylor|David Lee )$"), "PRIORITY").otherwise("CORE").alias("name_zone"),
    )
)
