from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating")
w2 = Window.orderBy(F.col("rating").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        "supplier_name",
        F.rank().over(w).alias("rating_rank"),
        F.last("supplier_name").over(w2).alias("last_supplier_name"),
    )
    .orderBy("supplier_name")
)
