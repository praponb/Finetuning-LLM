from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("pos").asc_nulls_first())
w2 = Window.partitionBy("customer_id").orderBy(F.col("adjusted_pos").desc())
w3 = Window.partitionBy("customer_id").orderBy(F.col("pos").desc_nulls_first())

exploded = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "li_item"))
    .select("id", "customer_id", "created_at", "pos", F.col("li_item.qty"))
)

filtered = (
    exploded
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & F.col("customer_id").isNotNull() & (F.col("qty") > 2))
    .select(
        "id",
        "customer_id",
        "pos",
        "qty",
        F.round((F.col("pos") * 1.07), 2).alias("adjusted_pos"),
        F.col("pos").cast("double").alias("pos_num"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.rank().over(w).alias("pos_rank"),
        F.cume_dist().over(w2).alias("adjusted_pos_cume_dist"),
        F.dense_rank().over(w3).alias("pos_dense_rank"),
    )
)

base = (
    scored
    .filter(F.col("customer_id").isNotNull())
    .withColumn("pos_rounded", F.round("pos", 2))
)

result = (
    base
    .select(
        "id",
        "pos",
        "qty",
        "adjusted_pos",
        "pos_rank",
        "adjusted_pos_cume_dist",
        "pos_dense_rank",
        "pos_rounded",
    )
    .orderBy("pos", F.col("adjusted_pos").desc())
)
