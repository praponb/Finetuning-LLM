from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region")
w2 = Window.partitionBy("region").orderBy("sqft", "id")
w_part = Window.partitionBy("city").orderBy("opened_date", "id")
w4 = Window.orderBy(F.col("sqft").desc(), "id")
w5 = Window.partitionBy("region").orderBy(F.col("seq_no").desc_nulls_first(), "id")
w6 = Window.partitionBy("region").orderBy(F.col("prev_sqft").desc(), "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())

kept = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") >= F.col("s.opened_date")), "left_anti")
)

eligible = (
    kept
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.opened_date")), "left_semi")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") <= F.col("s.opened_date")), "left_anti")
)

scored = (
    not_flagged
    .select(
        "*",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 4).alias("sqft_share"),
        F.lag("sqft", 1).over(w2).alias("prev_sqft"),
        F.row_number().over(w_part).alias("seq_no"),
        F.ntile(10).over(w4).alias("sqft_bucket"),
    )
)

ranked = (
    scored
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

deduped = (
    ranked
    .withColumn("row_num_2", F.row_number().over(w6))
    .filter(F.col("row_num_2") == 1)
    .drop("row_num_2")
)

latest_match = (
    deduped
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.city",
        "s.sqft_share",
        "s.seq_no",
        "s.sqft_bucket",
        F.col("c.amount"),
        F.col("c.order_date").alias("order_order_date"),
    )
)

result = (
    latest_match
    .select("id", "city", "seq_no", "sqft_bucket", "amount")
    .orderBy(F.col("amount").desc())
)
