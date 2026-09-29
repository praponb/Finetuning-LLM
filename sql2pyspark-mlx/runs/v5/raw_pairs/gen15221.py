from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)

scored = (
    df_suppliers
    .filter(F.col("rating") < 3.7)
    .select(
        "id",
        "country",
        "city",
        F.min("rating").over(w_running).alias("last4_min_rating"),
        F.max("rating").over(w_running).alias("last4_max_rating"),
        F.avg("rating").over(w_running).alias("last4_avg_rating"),
    )
)

result = (
    scored
    .select(
        "id",
        "country",
        "city",
        "last4_min_rating",
        "last4_max_rating",
        "last4_avg_rating",
    )
    .orderBy(F.col("last4_avg_rating").desc())
)
