from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("city", "country")

result = (
    df_customers
    .select(
        "id",
        "name",
        "city",
        "is_active",
        F.avg("credit_limit").over(w).alias("credit_limit_avg_5"),
        F.corr("age", "credit_limit").over(w2).alias("corr_age_credit_limit"),
    )
)
