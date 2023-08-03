from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalSpentByCustomer").master("local[*]").getOrCreate()

# Create schema when reading customer-orders
customerOrderSchema = StructType([ \
                                  StructField("cust_id", IntegerType(), True),
                                  StructField("item_id", IntegerType(), True),
                                  StructField("amount_spent", FloatType(), True)
                                  ])

# Load up the data into spark dataset
customersDF = spark.read.schema(customerOrderSchema).csv("file:///SparkCourse/customer-orders.csv")

totalByCustomer = customersDF.groupBy("cust_id").agg(func.round(func.sum("amount_spent"), 2) \
                                      .alias("total_spent"))

totalByCustomerSorted = totalByCustomer.sort("total_spent")

# totalByCustomerSorted.show(): This shows the default number of rows (usually 20 rows) from the DataFrame totalByCustomerSorted. It displays the result on the console, allowing you to visually inspect the data.
#
# totalByCustomerSorted.show(totalByCustomerSorted.count()): This shows all the rows present in the DataFrame totalByCustomerSorted. By passing totalByCustomerSorted.count() as an argument to the show() method, it specifies the number of rows to be shown. In this case, the count of the DataFrame is used, ensuring that all the rows are displayed.

totalByCustomerSorted.show(totalByCustomerSorted.count())

spark.stop()
