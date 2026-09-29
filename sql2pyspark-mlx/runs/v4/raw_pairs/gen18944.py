from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sqft_rounded", "id")
w2 = Window.partitionBy("city").orderBy("opened_date", "id")
w3 = Window.partitionBy("region").orderBy(F.col("sqft_rounded").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("city").orderBy(F.col("next_sqft_rounded").desc(), "id")

valid = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-04-19"))) & F.col("region").isNotNull() & F.col("sqft").between(9700, 15409))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.round("sqft", 1).alias("sqft_rounded"),
        F.coalesce("region", F.lit("unassigned")).alias("region_filled"),
    )
)

with_windows = (
    valid
    .withColumn("sqft_rounded_bucket", F.ntile(3).over(w))
    .withColumn("prev_sqft_rounded", F.lag("sqft_rounded", 1).over(w2))
    .withColumn("sqft_rounded_dense_rank", F.dense_rank().over(w3))
    .withColumn("next_sqft_rounded", F.lead("sqft_rounded", 2).over(w))
)

with_last = (
    with_windows
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

latest = (
    with_last
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft_rounded",
        "prev_sqft_rounded",
        "quantity",
        "inventory_last_restock",
        F.when(F.col("city").rlike("^(Berlin|Bangkok)$"), "GROUP_B").otherwise("EXTENDED").alias("city_zone"),
    )
    .orderBy(F.col("prev_sqft_rounded").desc_nulls_first())
)
