from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

with_last = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"), F.col("c.units").alias("store_sale_units"))
)

result = (
    with_last
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sale_date",
        "store_sale_units",
        F.when(F.col("city").rlike("^(Bangkok|Berlin)$"), "CORE").when(F.col("city").rlike("^(Chiang Mai|New York)$"), "GROUP_B").otherwise("GROUP_A").alias("city_zone"),
    )
    .orderBy(F.col("store_sale_units").desc())
)
