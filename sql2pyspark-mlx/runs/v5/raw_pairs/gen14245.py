from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock_quarter").desc_nulls_first(), "id")
w2 = Window.partitionBy("product_id").orderBy("cost", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy("last3_sum_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id")
w5 = Window.partitionBy("product_id").orderBy(F.col("last3_min_cost").desc_nulls_first())

cleaned = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-08-07").cast("date"))
    .select(
        "id",
        "product_id",
        "last_restock",
        F.quarter("last_restock").alias("last_restock_quarter"),
    )
)

latest = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "product_id")
)

enriched = (
    latest
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.brand").isNotNull())
    .select("s.id", "s.product_id", F.col("p.cost"))
)

enriched_inventory = (
    enriched
    .select(
        "*",
        F.avg("cost").over(w2).alias("last3_avg_cost"),
        F.sum("cost").over(w2).alias("last3_sum_cost"),
        F.max("cost").over(w2).alias("last3_max_cost"),
        F.min("cost").over(w2).alias("last3_min_cost"),
    )
)

scored = (
    enriched_inventory
    .select(
        "*",
        F.sum("last3_min_cost").over(w3).alias("cumulative_last3_min_cost"),
        F.round((F.col("last3_max_cost") / F.sum("last3_max_cost").over(w4)), 4).alias("last3_max_cost_share"),
        F.cume_dist().over(w5).alias("last3_min_cost_cume_dist"),
    )
)

result = (
    scored
    .select(
        "id",
        "product_id",
        F.col("last3_avg_cost").alias("LAST3_AVG_COST_VAL"),
        F.col("last3_min_cost_cume_dist").alias("LAST3_MIN_COST_CUME_DIST"),
        F.col("last3_min_cost").alias("LAST3_MIN_COST_VAL"),
    )
    .unpivot(
        ids=["id", "product_id"],
        values=["LAST3_AVG_COST_VAL", "LAST3_MIN_COST_CUME_DIST", "LAST3_MIN_COST_VAL"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
)
