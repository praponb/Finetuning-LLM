from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.orderBy("rating", "id").rowsBetween(-5, -1)

base = (
    df_suppliers
    .filter(F.col("rating").between(2.0, 3.7))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.substring("supplier_name", 1, 4).alias("supplier_name_prefix"),
    )
)

result = (
    base
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.round((F.col("rating") / F.sum("rating").over(w)), 4).alias("rating_share"),
        F.avg("rating").over(w2).alias("rating_avg_5_prior"),
    )
)
