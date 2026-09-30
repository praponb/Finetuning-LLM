from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.partitionBy("city").orderBy("age", "id")

enriched = (
    df_customers
    .filter(F.col("credit_limit").isNull())
    .withColumn("credit_limit_bucket", F.ntile(5).over(w))
    .withColumn("next_credit_limit", F.lead("credit_limit", 1).over(w2))
)

result = (
    enriched
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        "credit_limit_bucket",
    )
)
