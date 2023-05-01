SELECT COUNT(DISTINCT Bid.user_id)
FROM (Bid
INNER JOIN Products ON Bid.user_id = Products.user_id);