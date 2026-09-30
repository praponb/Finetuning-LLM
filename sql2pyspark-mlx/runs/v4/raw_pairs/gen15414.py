from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_first())

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select("*", F.trim("supplier_name").alias("clean_supplier_name"))
)

result = (
    valid
    .select(
        "*",
        F.round((F.col("rating") / F.sum("rating").over(w)), 4).alias("rating_share"),
        F.cume_dist().over(w2).alias("rating_cume_dist"),
    )
)
