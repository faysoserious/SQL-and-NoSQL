from pymongo import MongoClient
import pprint

# Making a connection with MongoClient
client = MongoClient('localhost',27017)
#Getting a Database
db = client.Northwind
#Getting a collection
#db.customers.aggregate returns command_cursor which can not print directly
#use for ...in... to print
with open('NOSQL_5_1.txt', 'wt') as f:
    for orderid in db.orders.aggregate([
    #specifies the collection in the same database to perform the join with
    #        {"$lookup":{"from": "orders","localField": "CustomerID","foreignField": "CustomerID","as": "Customeritems"}},
    
            {"$lookup":{"from": "order-details","localField": "OrderID","foreignField": "OrderID","as": "orderdetails"}},
                
    #         {"$project":{"ProductID":1}},
            {"$unwind":{ "path":"$orderdetails"}},
            {"$lookup":{"from": "Products","localField": "orderdetails","foreignField": "ProductID","as": "productsdetails"}},
            
    #select the columns that we need
           
            {"$project":{"_id":0,"CustomerID":1,"OrderID":1,"orderdetails":{"ProductID":1,"productsdetails":{"ProductName":1}}}},
    
            {"$match" : {"CustomerID":"ALFKI"}}
                            ]):
    #Support to pretty-print lists, tuples, & dictionaries recursively.
       print(orderid, file = f)
