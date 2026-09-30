from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)

valid = (
    df_suppliers
    .filter((F.col("city").eqNullSafe(F.lit("New York"))) & F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.initcap(F.trim("city")).alias("city_title"),
    )
)

result = (
    valid
    .select(
        "id",
        "supplier_name",
        F.avg("rating").over(w).alias("last4_avg_rating"),
        F.max("rating").over(w).alias("last4_max_rating"),
        F.sum("rating").over(w).alias("last4_sum_rating"),
    )
)
