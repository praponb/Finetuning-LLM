from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit", "id")

result = (
    df_customers
    .select(
        "id",
        "city",
        "credit_limit",
        "is_active",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 5)
    .select("id", "city", "credit_limit", "is_active")
)
