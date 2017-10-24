# -*- coding: utf-8 -*-
import sqlite3
import itertools

# Learned from http://peiqiang.net/2016/02/05/python-sqlite3.html
# DICT_GEN() build the result of querying into dictionary
def dict_gen(c):
    # get the name of each colum
    filed_names = [d[0] for d in c.description]
    while True:
        row = c.fetchone()
        if not row: return
        yield dict(itertools.zip_longest(filed_names,row))
        
# apply "with" instead of doing commit the operation and close the database
with sqlite3.connect('northwind.db') as conn:
    #create a Cursor object and call its execute() method to perform SQL commands
    c = conn.cursor()
    #Solve the Error "Could not decode to UTF-8 column 'ProductName'"
    conn.text_factory = bytes
    CustomerID = ('ALFKI',)
    Result=list()

# From the diagram for the database we can figure that
# the column CustomerID from table Customers is the key connected to table Orders
# the column OrderID from table Orders is the key connected to table Orders Details
# the column ProductID from table Order Details is the key connected to table Products
# table Products contains ProductName
# Get the OrderID of Orders that contains more than two different Products
    c.execute("""SELECT Orders.OrderID
                                
                          FROM Customers 
                          join Orders on Orders.CustomerID = Customers.CustomerID
                          join "Order Details" on "Order Details".OrderID = Orders.OrderID
                          join Products on Products. ProductID = "Order Details".ProductID
                          WHERE Customers.CustomerID=?
                          GROUP BY Orders.OrderID
                          Having count(Orders.OrderID)>1
                          """
                          , CustomerID)   
    Orders =c.fetchall()
# Get the Product info according to the OrderIDs
    for i in range(len(Orders)):
    
        c.execute("""SELECT "Order Details".OrderID, 
                    "Order Details".ProductID,
                    Products.ProductName
                          FROM "Order Details"
                          join Products on Products. ProductID = "Order Details".ProductID
                          WHERE "Order Details".OrderID=?
                          """
                          , Orders[i])
        Result.append(c.fetchall())
#output the result into txt file
with open('SQLite_5_2.txt', 'wt') as f:
  
    for item in Result:
            print(item, file =f)
             
                