from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("city", "region").orderBy(F.col("order_order_date").desc(), "id")

ranked = (
    df_stores
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest_match = (
    ranked
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.amount").alias("order_amount"),
        F.col("c.order_date").alias("order_order_date"),
    )
)

deduped = (
    latest_match
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

kept = (
    deduped
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") > "2024-03-01"), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        "rank_in_group",
        "order_order_date",
        "row_num",
        F.when(F.col("city").rlike("^(Bangkok|Singapore)$"), "EXTENDED").when(F.col("city").rlike("^(Chiang Mai|New York)$"), "CORE").otherwise("OTHER_ZONE").alias("city_zone"),
    )
)
