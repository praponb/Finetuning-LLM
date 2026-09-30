from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("product_id").orderBy("sale_date_plus_months", "id")
w_running = Window.orderBy(F.col("units").desc_nulls_first())
w_ordered = Window.orderBy("sale_date", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("units_avg_2", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("sale_date_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy("units_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("store_id").orderBy("first_known_units_avg_2", "id")

filtered = (
    df_store_sales
    .filter(F.col("store_id") == 4)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        F.add_months("sale_date", 3).alias("sale_date_plus_months"),
        F.coalesce("units", F.lit(0)).alias("units_filled"),
    )
)

with_windows = (
    filtered
    .filter(F.col("store_id").isin([1, 2, 5, 6]))
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        F.lead("units", 1).over(w_grp).alias("next_units"),
        F.rank().over(w_running).alias("units_rank"),
        F.avg("units").over(w_ordered).alias("units_avg_2"),
    )
)

valid = (
    with_windows
    .filter(F.col("units_avg_2").between(16, 20))
    .select(
        "*",
        F.round((F.col("units_avg_2") * 1.07), 2).alias("units_avg_2_adjusted"),
        F.quarter("sale_date").alias("sale_date_quarter"),
    )
)

with_windows_store_sales = (
    valid
    .select(
        "id",
        "store_id",
        "units",
        "units_rank",
        "units_avg_2",
        "units_avg_2_adjusted",
        "sale_date_quarter",
        F.avg("units_rank").over(w4).alias("units_rank_avg_3"),
        F.first("units_avg_2", ignorenulls=True).over(w5).alias("first_known_units_avg_2"),
        F.sum("sale_date_quarter").over(w6).alias("cumulative_sale_date_quarter"),
    )
)

deduped = (
    with_windows_store_sales
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("id", "store_id", "units_rank", "units_rank_avg_3")
)

kept_keys = (
    deduped
    .select("store_id", "id")
    .subtract(
        df_store_sales
        .filter(F.col("product_id").isin([3, 7, 10]))
        .select("store_id", "id")
    )
)

verified = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

summary = (
    verified
    .withColumn("units_rank_avg_3_tier", F.when(F.col("units_rank_avg_3") >= 8, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["store_id", "units_rank_avg_3_tier"], ["store_id"], []],
        "store_id",
        "units_rank_avg_3_tier",
    )
    .agg(
        F.round(F.avg("units_rank"), 2).alias("avg_units_rank"),
        F.count("*").alias("num_rows"),
        F.max("units_rank_avg_3").alias("max_units_rank_avg_3"),
        F.grouping("store_id").alias("gid"),
    )
)

result = (
    summary
    .select("units_rank_avg_3_tier", "avg_units_rank", "num_rows", "gid")
    .orderBy(F.col("num_rows").desc_nulls_first(), "units_rank_avg_3_tier")
)
