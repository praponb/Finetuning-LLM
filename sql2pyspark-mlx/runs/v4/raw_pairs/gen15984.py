from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("country").orderBy("rating", "id")
w3 = Window.orderBy("first_known_rating", "id")

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("country_supplier_name", F.concat("country", F.lit("/"), "supplier_name"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("OTHER")))
)

eligible = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.supplier_id").isNull()), "left_anti")
)

scored = (
    eligible
    .withColumn("first_known_rating", F.first("rating", ignorenulls=True).over(w_hist))
    .withColumn("first_known_city", F.first("city", ignorenulls=True).over(w_hist))
)

not_flagged = (
    scored
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.length(F.col("x.product_name")) > 14), "left_anti")
)

result = (
    not_flagged
    .withColumn("prev_first_known_rating", F.lag("first_known_rating", 1, 0).over(w_part))
    .withColumn("prev_first_known_rating_2", F.lag("first_known_rating", 1).over(w3))
)
