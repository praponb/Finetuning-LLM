from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())

with_latest = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.discount"))
)

result = (
    with_latest
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.when(F.col("city").rlike("^(New York|Bangkok)$"), "GROUP_A").when(F.col("city").rlike("^(Chiang Mai|Singapore)$"), "OTHER_ZONE").otherwise("CORE").alias("city_zone"),
    )
    .orderBy(F.col("city_zone").desc_nulls_last())
)
