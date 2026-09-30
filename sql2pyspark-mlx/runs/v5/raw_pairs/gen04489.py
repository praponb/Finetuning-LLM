from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

with_last = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.region",
        "s.sqft",
        F.col("c.revenue"),
        F.col("c.sale_date"),
    )
)

result = (
    with_last
    .select(
        "id",
        "city",
        "region",
        "sqft",
        "sale_date",
        F.when(F.col("city").rlike("^(Chiang Mai|New York)$"), "GROUP_B").when(F.col("city").rlike("^(Bangkok|Singapore)$"), "PRIORITY").otherwise("EXTENDED").alias("city_zone"),
    )
)
