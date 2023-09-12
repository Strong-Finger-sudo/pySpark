from pyspark.sql import SparkSession
from pyspark.sql.connect.functions import collect_list
from pyspark.sql.functions import explode, col

spark = SparkSession.builder.appName("ProductCategoryPairs").getOrCreate()

categories_with_products = categories_df.join(products_df, on="CategoryID", how="left").groupBy("CategoryName").agg(collect_list("ProductName").alias("Products"))

expanded_df = categories_with_products.select(
    "CategoryName", explode("Products").alias("ProductName")
)

result_df = expanded_df.select("ProductName", "CategoryName")

result_df.show()