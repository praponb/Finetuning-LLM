from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating", "id")
w2 = Window.orderBy("rating", "id")
w3 = Window.partitionBy("city")

base = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .select(
        "*",
        F.substring("supplier_name", 1, 3).alias("supplier_name_prefix"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

result = (
    base
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("next_rating", F.lead("rating", 1).over(w2))
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w3)), 4))
)
