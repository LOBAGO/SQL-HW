select CategoryName,Count(*),Round(AVG(UnitPrice),2),MIN(UnitPrice),MAX(Unitprice),SUM(UnitsOnOrder)
from Product Inner Join Category on CategoryId = Category.Id
Group By CategoryId 
Having Count(*)>10
Order By CategoryId
;

