from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_last(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

base = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.lower("supplier_name").like("%oky%"))
    .withColumn("supplier_name_normalized", F.upper(F.trim("supplier_name")))
    .withColumn("city_filled", F.coalesce("city", F.lit("OTHER")))
)

deduped = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

deduped_suppliers = (
    deduped
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

with_latest = (
    deduped_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.brand"),
        F.col("c.launch_date").alias("product_launch_date"),
    )
)

result = (
    with_latest
    .groupBy("city")
    .pivot("brand", ["Initech", "Globex"])
    .agg(F.max("rating").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "city",
        F.col("Initech_amount").alias("initech_amount"),
        F.col("Initech_n").alias("initech_n"),
        F.col("Globex_amount").alias("globex_amount"),
        F.col("Globex_n").alias("globex_n"),
    )
)
