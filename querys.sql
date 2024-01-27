/* QUERY 1 */

SELECT SalesOrderID, COUNT(*)
FROM SalesOrderDetail
GROUP BY SalesOrderID
HAVING COUNT(*) >= 3


/* QUERY 2 */
SELECT COUNT(t2.OrderQty), t3.Name, t3.DaysToManufacture
FROM SpecialOfferProduct t1
INNER JOIN Production t3 on t1.ProductID = t3.ProductID
INNER JOIN SalesOrderDetail t2 on t3.ProductID = t2.ProductID
GROUP BY t2.OrderQty, t3.DaysToManufacture, t3.Name
ORDER BY t2.OrderQty ASC LIMIT 3