from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc())
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_suppliers
    .select(
        "id",
        "rating",
        "country",
        "supplier_name",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.last("supplier_name").over(w2).alias("last_supplier_name"),
    )
)
