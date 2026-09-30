from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_customers
    .filter(F.col("country") == "UK")
    .select(
        "id",
        "segment",
        "credit_limit",
        "country",
        F.last("country").over(w).alias("last_country"),
    )
)
