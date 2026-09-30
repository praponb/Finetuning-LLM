from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc())
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        "city",
        F.percent_rank().over(w).alias("rating_pct_rank"),
        F.last("country").over(w2).alias("last_country"),
    )
)
