from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("product_id", "store_id")
w4 = Window.partitionBy("store_id", "city").orderBy(F.col("revenue_share").desc_nulls_last())
w5 = Window.partitionBy("store_city", "store_id").orderBy(F.col("dedup_rank").desc(), "id")
w6 = Window.partitionBy("store_city").orderBy("units", "id")
w7 = Window.partitionBy("store_id").orderBy("sqft", "id")
w8 = Window.partitionBy("city").orderBy("units", "id").rowsBetween(-6, -1)
w9 = Window.partitionBy("city", "store_city").orderBy("product_id_count_units", "id")

cleaned = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 4, 6]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.when(F.col("revenue") >= 4400, "High").when(F.col("revenue") >= 700, "Medium").otherwise("Low").alias("revenue_band"),
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue_band",
        F.avg("revenue").over(w).alias("revenue_avg_6_prior"),
        F.round((F.col("revenue") / F.sum("revenue").over(w2)), 4).alias("revenue_share"),
        F.count("units").over(w3).alias("product_id_count_units"),
    )
)

enriched = (
    with_windows
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.units",
        "e.revenue_avg_6_prior",
        "e.revenue_share",
        "e.product_id_count_units",
        F.col("p.store_name"),
        F.col("p.city"),
    )
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

joined = (
    deduped
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "inner")
    .select(
        "s.*",
        F.col("b.store_name").alias("store_store_name"),
        F.col("b.sqft"),
        F.col("b.city").alias("store_city"),
    )
)

deduped_store_sales = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

enriched_store_sales = (
    deduped_store_sales
    .select(
        "id",
        "units",
        "product_id_count_units",
        "store_name",
        "city",
        "store_store_name",
        "sqft",
        "store_city",
        "rank_in_group",
        F.lead("revenue_share", 2).over(w6).alias("next_revenue_share"),
        F.ntile(5).over(w7).alias("sqft_bucket"),
        F.avg("sqft").over(w8).alias("sqft_avg_6_prior"),
        F.row_number().over(w9).alias("seq_no"),
    )
)

result = (
    enriched_store_sales
    .select(
        "id",
        "store_name",
        "store_store_name",
        "rank_in_group",
        "sqft_avg_6_prior",
        "seq_no",
        F.when(F.col("store_name").rlike("^(Siam Flagship|Old Town)$"), "EXTENDED").when(F.col("store_name").rlike("^(Downtown Express|Airport Kiosk)$"), "GROUP_A").otherwise("GROUP_B").alias("store_name_zone"),
    )
    .orderBy(F.col("seq_no").desc_nulls_first(), F.col("id").desc())
)
