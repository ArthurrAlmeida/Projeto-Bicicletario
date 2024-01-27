import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node SpecialOfferProduct
SpecialOfferProduct_node1706026168817 = glueContext.create_dynamic_frame.from_catalog(database="rox", table_name="sales_specialofferproduct_csv", transformation_ctx="SpecialOfferProduct_node1706026168817")

# Script generated for node Person
Person_node1706025849860 = glueContext.create_dynamic_frame.from_catalog(database="rox", table_name="person_person_csv", transformation_ctx="Person_node1706025849860")

# Script generated for node Production
Production_node1706125623570 = glueContext.create_dynamic_frame.from_catalog(database="rox", table_name="production_product_csv", transformation_ctx="Production_node1706125623570")

# Script generated for node Customer
Customer_node1706025888925 = glueContext.create_dynamic_frame.from_catalog(database="rox", table_name="sales_customer_csv", transformation_ctx="Customer_node1706025888925")

# Script generated for node SalesOrderHeader
SalesOrderHeader_node1706124587759 = glueContext.create_dynamic_frame.from_catalog(database="rox", table_name="salesorderheader_csv", transformation_ctx="SalesOrderHeader_node1706124587759")

# Script generated for node SalesOrderDetail
SalesOrderDetail_node1706025935836 = glueContext.create_dynamic_frame.from_catalog(database="rox", table_name="sales_salesorderdetail_csv", transformation_ctx="SalesOrderDetail_node1706025935836")

# Script generated for node OfferProductSchema
OfferProductSchema_node1706026282580 = ApplyMapping.apply(frame=SpecialOfferProduct_node1706026168817, mappings=[("specialofferid", "long", "specialofferid", "long"), ("productid", "long", "productid", "long"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "timestamp")], transformation_ctx="OfferProductSchema_node1706026282580")

# Script generated for node PersonSchema
PersonSchema_node1706025950163 = ApplyMapping.apply(frame=Person_node1706025849860, mappings=[("businessentityid", "long", "businessentityid", "long"), ("persontype", "string", "persontype", "string"), ("namestyle", "long", "namestyle", "long"), ("title", "string", "title", "string"), ("firstname", "string", "firstname", "string"), ("middlename", "string", "middlename", "string"), ("lastname", "string", "lastname", "string"), ("suffix", "string", "suffix", "string"), ("emailpromotion", "long", "emailpromotion", "long"), ("additionalcontactinfo", "string", "additionalcontactinfo", "string"), ("demographics", "string", "demographics", "string"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "timestamp")], transformation_ctx="PersonSchema_node1706025950163")

# Script generated for node ProductionSchema
ProductionSchema_node1706125650399 = ApplyMapping.apply(frame=Production_node1706125623570, mappings=[("productid", "long", "productid", "long"), ("name", "string", "name", "string"), ("productnumber", "string", "productnumber", "string"), ("makeflag", "long", "makeflag", "long"), ("finishedgoodsflag", "long", "finishedgoodsflag", "long"), ("color", "string", "color", "string"), ("safetystocklevel", "long", "safetystocklevel", "long"), ("reorderpoint", "long", "reorderpoint", "long"), ("standardcost", "string", "standardcost", "string"), ("listprice", "string", "listprice", "string"), ("size", "string", "size", "string"), ("sizeunitmeasurecode", "string", "sizeunitmeasurecode", "string"), ("weightunitmeasurecode", "string", "weightunitmeasurecode", "string"), ("weight", "double", "weight", "string"), ("daystomanufacture", "long", "daystomanufacture", "long"), ("productline", "string", "productline", "string"), ("class", "string", "class", "string"), ("style", "string", "style", "string"), ("productsubcategoryid", "long", "productsubcategoryid", "string"), ("productmodelid", "long", "productmodelid", "string"), ("sellstartdate", "string", "sellstartdate", "timestamp"), ("sellenddate", "string", "sellenddate", "timestamp"), ("discontinueddate", "string", "discontinueddate", "timestamp"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "timestamp")], transformation_ctx="ProductionSchema_node1706125650399")

# Script generated for node CustomerSchema
CustomerSchema_node1706025989218 = ApplyMapping.apply(frame=Customer_node1706025888925, mappings=[("customerid", "long", "customerid", "long"), ("personid", "long", "personid", "string"), ("storeid", "long", "storeid", "string"), ("territoryid", "long", "territoryid", "string"), ("accountnumber", "string", "accountnumber", "string"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "timestamp")], transformation_ctx="CustomerSchema_node1706025989218")

# Script generated for node HeaderSchema
HeaderSchema_node1706124665258 = ApplyMapping.apply(frame=SalesOrderHeader_node1706124587759, mappings=[("salesorderid", "long", "salesorderid", "long"), ("revisionnumber", "long", "revisionnumber", "long"), ("orderdate", "string", "orderdate", "timestamp"), ("duedate", "string", "duedate", "timestamp"), ("shipdate", "string", "shipdate", "timestamp"), ("status", "long", "status", "long"), ("onlineorderflag", "long", "onlineorderflag", "long"), ("salesordernumber", "string", "salesordernumber", "string"), ("purchaseordernumber", "string", "purchaseordernumber", "string"), ("accountnumber", "string", "accountnumber", "string"), ("customerid", "long", "customerid", "long"), ("salespersonid", "long", "salespersonid", "choice"), ("territoryid", "long", "territoryid", "long"), ("billtoaddressid", "long", "billtoaddressid", "long"), ("shiptoaddressid", "long", "shiptoaddressid", "long"), ("shipmethodid", "long", "shipmethodid", "long"), ("creditcardid", "long", "creditcardid", "choice"), ("creditcardapprovalcode", "string", "creditcardapprovalcode", "string"), ("currencyrateid", "long", "currencyrateid", "choice"), ("subtotal", "string", "subtotal", "string"), ("taxamt", "string", "taxamt", "string"), ("freight", "string", "freight", "string"), ("totaldue", "string", "totaldue", "string"), ("comment", "string", "comment", "string"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "timestamp")], transformation_ctx="HeaderSchema_node1706124665258")

# Script generated for node DetailSchema
DetailSchema_node1706026111240 = ApplyMapping.apply(frame=SalesOrderDetail_node1706025935836, mappings=[("salesorderid", "long", "salesorderid", "long"), ("salesorderdetailid", "long", "salesorderdetailid", "long"), ("carriertrackingnumber", "string", "carriertrackingnumber", "string"), ("orderqty", "long", "orderqty", "long"), ("productid", "long", "productid", "long"), ("specialofferid", "long", "specialofferid", "long"), ("unitprice", "string", "unitprice", "string"), ("unitpricediscount", "string", "unitpricediscount", "string"), ("linetotal", "double", "linetotal", "long"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "timestamp")], transformation_ctx="DetailSchema_node1706026111240")

# Script generated for node SpecialOfferProduct
SpecialOfferProduct_node1706026906329 = glueContext.write_dynamic_frame.from_options(frame=OfferProductSchema_node1706026282580, connection_type="s3", format="glueparquet", connection_options={"path": "s3://datalake-rox-proj/datalake/SpecialOfferProduct/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="SpecialOfferProduct_node1706026906329")

# Script generated for node Person
Person_node1706026340913 = glueContext.write_dynamic_frame.from_options(frame=PersonSchema_node1706025950163, connection_type="s3", format="glueparquet", connection_options={"path": "s3://datalake-rox-proj/datalake/Person/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Person_node1706026340913")

# Script generated for node Production
Production_node1706125694047 = glueContext.write_dynamic_frame.from_options(frame=ProductionSchema_node1706125650399, connection_type="s3", format="glueparquet", connection_options={"path": "s3://datalake-rox-proj/datalake/Production/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Production_node1706125694047")

# Script generated for node Customer
Customer_node1706026888757 = glueContext.write_dynamic_frame.from_options(frame=CustomerSchema_node1706025989218, connection_type="s3", format="glueparquet", connection_options={"path": "s3://datalake-rox-proj/datalake/Customer/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Customer_node1706026888757")

# Script generated for node SalesOrderheader
SalesOrderheader_node1706124830354 = glueContext.write_dynamic_frame.from_options(frame=HeaderSchema_node1706124665258, connection_type="s3", format="glueparquet", connection_options={"path": "s3://datalake-rox-proj/datalake/SalesOrderHeader/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="SalesOrderheader_node1706124830354")

# Script generated for node SalesOrderDetail
SalesOrderDetail_node1706026894639 = glueContext.write_dynamic_frame.from_options(frame=DetailSchema_node1706026111240, connection_type="s3", format="glueparquet", connection_options={"path": "s3://datalake-rox-proj/datalake/SalesOrderDetail/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="SalesOrderDetail_node1706026894639")

job.commit()