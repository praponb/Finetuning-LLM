from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("credit_limit").desc_nulls_first())
w_ordered = Window.partitionBy("country").orderBy("credit_limit", "id")

result = (
    df_customers
    .select(
        "id",
        "name",
        "email",
        "is_active",
        F.percent_rank().over(w).alias("credit_limit_percent_rank"),
        F.ntile(10).over(w_ordered).alias("credit_limit_bucket"),
    )
)
