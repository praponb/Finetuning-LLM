from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id")
w2 = Window.orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("adjusted_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_suppliers
    .filter(F.length("supplier_name") >= 7)
    .select("*", F.ceil("rating").alias("rating_whole"))
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

eligible = (
    deduped
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("x.tags"), "new")), "left_anti")
)

base = (
    eligible
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "rating",
        "rn",
        F.round((F.col("rating") * 0.85), 2).alias("adjusted_rating"),
    )
)

eligible_suppliers = (
    base
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.category").isin(["Electronics", "Home", "Grocery"])), "left_anti")
)

result = (
    eligible_suppliers
    .filter(F.col("rating") >= 2.1)
    .withColumn("cumulative_adjusted_rating", F.sum("adjusted_rating").over(w2))
    .withColumn("running_adjusted_rating", F.sum("adjusted_rating").over(w3))
)
