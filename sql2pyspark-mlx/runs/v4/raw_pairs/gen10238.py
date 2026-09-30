from pyspark.sql import functions as F

src = (
    df_products
    .filter((F.col("price") <= 560) & (F.col("supplier_id") == 29))
    .withColumn("margin", (F.col("price") * F.col("cost")))
)

summary = (
    src
    .groupBy("category")
    .agg(
        F.sum(F.when(F.col("brand") == "Globex", F.col("margin")).otherwise(0)).alias("globex_margin"),
    )
)

result = summary.orderBy(F.col("globex_margin").desc())
