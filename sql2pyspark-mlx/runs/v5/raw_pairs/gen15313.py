from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .filter(F.col("suppliers.city").isin(["Singapore", "New York", "Bangkok"]))
    .groupBy(
        F.when(F.col("suppliers.country").isin(["UK", "DE"]), "priority").otherwise("other").alias("country_group"),
    )
    .agg(
        F.round(F.avg(F.col("products.price")), 2).alias("avg_price"),
        F.count(F.col("products.cost")).alias("cost_count"),
    )
    .orderBy("avg_price")
)
