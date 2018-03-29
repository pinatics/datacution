SET SESSION group_concat_max_len = 3000000;

DROP TABLE IF EXISTS RFMTemp;
DROP TABLE IF EXISTS Rpercentiles;
DROP TABLE IF EXISTS Fpercentiles;
DROP TABLE IF EXISTS Mpercentiles;
DROP TABLE IF EXISTS RFMScore;
DROP PROCEDURE IF EXISTS create_rfm_temp_table;

CREATE PROCEDURE create_rfm_temp_table()
CREATE TABLE RFMTemp AS
SELECT UserID,
       DATEDIFF(CURDATE(),
                last_order_date) -
  (SELECT min(DATEDIFF(CURDATE(),last_order_date))
   FROM
     (SELECT UserID,
             max(STR_TO_DATE(TransactionDate,'%d-%M-%Y')) AS last_order_date,
             min(STR_TO_DATE(TransactionDate,'%d-%M-%Y')) AS min_order_date,
             Count(TransactionID) AS count_order,
             sum(Revenue) AS total_amount
      FROM trans
      GROUP BY UserID) X) AS recency,
       count_order AS frequency,
       total_amount AS monetary
FROM
  (SELECT UserID,
          max(STR_TO_DATE(TransactionDate,'%d-%M-%Y')) AS last_order_date,
          Count(TransactionID) AS count_order,
          sum(Revenue) AS total_amount
   FROM trans
   GROUP BY UserID) T;

CALL create_rfm_temp_table();

DELIMITER $$

DROP PROCEDURE IF EXISTS make_ntile$$
CREATE PROCEDURE make_ntile(IN db_name VARCHAR(40), IN tbl_name VARCHAR(40), IN amount float, IN part_label VARCHAR(10), OUT SEGMENT VARCHAR(200)) BEGIN
SET SEGMENT = CONCAT( 'cast(substring_index(', 'substring_index(', 'group_concat(`',db_name,'`.`',tbl_name,'` ', 'order by `',db_name,'`.`',tbl_name,'` ', 'ASC separator \',\'', '),', '\',\',', '((',amount,' * count(0)) + 1)', '),', '\',\',', '-(1)) as decimal(18,0)', ') AS `',part_label,'`,' ); END$$

DELIMITER ;

DELIMITER $$
DROP PROCEDURE IF EXISTS make_rfm_component$$
CREATE PROCEDURE make_rfm_component( IN comp_tbl_name VARCHAR(40), IN db_name VARCHAR(40), IN tbl_name VARCHAR(40)) BEGIN
SET @p1 = CONCAT('CREATE TABLE ',comp_tbl_name,' AS SELECT ROUND(min(`',db_name,'`.`',tbl_name,'`)) as P1,'); CALL make_ntile(db_name, tbl_name, 0.20, 'P2', @p2); CALL make_ntile(db_name, tbl_name, 0.40, 'P3', @p3); CALL make_ntile(db_name, tbl_name, 0.60, 'P4', @p4); CALL make_ntile(db_name, tbl_name, 0.80, 'P5', @p5);
SET @p6 = CONCAT('ROUND(max(`',db_name,'`.`',tbl_name,'`)) as P6 FROM ',db_name,';');
SET @component = CONCAT(@p1,@p2,@p3,@p4,@p5,@p6); PREPARE stmt
FROM @component; EXECUTE stmt; DEALLOCATE PREPARE stmt; END$$
DELIMITER ;

CALL make_rfm_component('Rpercentiles', 'RFMTemp', 'recency');

CALL make_rfm_component('Fpercentiles', 'RFMTemp', 'frequency');

CALL make_rfm_component('Mpercentiles', 'RFMTemp', 'monetary');

DROP PROCEDURE IF EXISTS rfm_score;

CREATE PROCEDURE rfm_score()
CREATE TABLE RFMScore AS
SELECT a.UserID,
       a.RScore,
       b.FScore,
       c.MScore,
       ((a.RScore * 100) + (b.FScore * 10) + c.MScore) AS RFM_Score
FROM
  (SELECT UserID,
          recency,
          CASE
              WHEN recency BETWEEN
                     (SELECT P1
                      FROM Rpercentiles) AND
                     (SELECT P2
                      FROM Rpercentiles) THEN 1
              WHEN recency BETWEEN
                     (SELECT P2
                      FROM Rpercentiles) AND
                     (SELECT P3
                      FROM Rpercentiles) THEN 2
              WHEN recency BETWEEN
                     (SELECT P3
                      FROM Rpercentiles) AND
                     (SELECT P4
                      FROM Rpercentiles) THEN 3
              WHEN recency BETWEEN
                     (SELECT P4
                      FROM Rpercentiles) AND
                     (SELECT P5
                      FROM Rpercentiles) THEN 4
              WHEN recency BETWEEN
                     (SELECT P5
                      FROM Rpercentiles) AND
                     (SELECT P6
                      FROM Rpercentiles) THEN 5
              ELSE NULL
          END AS RScore
   FROM RFMTemp) AS a,

  (SELECT UserID,
          frequency,
          CASE
              WHEN frequency BETWEEN
                     (SELECT P1
                      FROM Fpercentiles) AND
                     (SELECT P2
                      FROM Fpercentiles) THEN 1
              WHEN frequency BETWEEN
                     (SELECT P2
                      FROM Fpercentiles) AND
                     (SELECT P3
                      FROM Fpercentiles) THEN 2
              WHEN frequency BETWEEN
                     (SELECT P3
                      FROM Fpercentiles) AND
                     (SELECT P4
                      FROM Fpercentiles) THEN 3
              WHEN frequency BETWEEN
                     (SELECT P4
                      FROM Fpercentiles) AND
                     (SELECT P5
                      FROM Fpercentiles) THEN 4
              WHEN frequency BETWEEN
                     (SELECT P5
                      FROM Fpercentiles) AND
                     (SELECT P6
                      FROM Fpercentiles) THEN 5
              ELSE NULL
          END AS FScore
   FROM RFMTemp) AS b,

  (SELECT UserID,
          monetary,
          CASE
              WHEN monetary BETWEEN
                     (SELECT P1
                      FROM Mpercentiles) AND
                     (SELECT P2
                      FROM Mpercentiles) THEN 1
              WHEN monetary BETWEEN
                     (SELECT P2
                      FROM Mpercentiles) AND
                     (SELECT P3
                      FROM Mpercentiles) THEN 2
              WHEN monetary BETWEEN
                     (SELECT P3
                      FROM Mpercentiles) AND
                     (SELECT P4
                      FROM Mpercentiles) THEN 3
              WHEN monetary BETWEEN
                     (SELECT P4
                      FROM Mpercentiles) AND
                     (SELECT P5
                      FROM Mpercentiles) THEN 4
              WHEN monetary BETWEEN
                     (SELECT P5
                      FROM Mpercentiles) AND
                     (SELECT P6
                      FROM Mpercentiles) THEN 5
              ELSE NULL
          END AS MScore
   FROM RFMTemp) AS c
WHERE a.UserID = b.UserID
  AND a.UserID = c.UserID
ORDER BY RFM_Score DESC;

CALL rfm_score();
