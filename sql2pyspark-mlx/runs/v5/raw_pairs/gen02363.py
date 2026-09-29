from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

base = (
    df_suppliers
    .filter(~F.col("country").isin(["DE", "US", "JP"]))
    .withColumn("rating_int", F.col("rating").cast("int"))
)

with_last = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.country", "s.rating_int", F.col("c.cost"))
)

latest = (
    with_last
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

with_last_suppliers = (
    latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.rating_int",
        "s.cost",
        "s.dedup_rank",
        F.col("c.price"),
    )
)

with_latest = (
    with_last_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.cost").alias("product_cost"),
        F.col("c.brand").alias("product_brand"),
    )
)

result = (
    with_latest
    .groupBy("country")
    .agg(
        F.count_if(F.col("product_brand") == "Globex").alias("globex_events"),
        F.round(F.sum("product_cost") / F.nullif(F.count(F.when(F.col("product_brand") == "Acme", 1)), F.lit(0)), 2).alias("product_cost_per_acme"),
    )
)
