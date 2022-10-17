UPDATE FUNC_RU
  SET RESOURCE_URI = CONCAT('s3a', SUBSTR(RESOURCE_URI, 4, LENGTH(RESOURCE_URI)))
WHERE
      RESOURCE_URI LIKE 's3n://%'
;

UPDATE SKEWED_COL_VALUE_LOC_MAP
  SET LOCATION = CONCAT('s3a', SUBSTR(LOCATION, 4, LENGTH(LOCATION)))
WHERE
      LOCATION LIKE 's3n://%'
;

UPDATE SDS
  SET LOCATION = CONCAT('s3a', SUBSTR(LOCATION, 4, LENGTH(LOCATION)))
WHERE
      LOCATION LIKE 's3n://%'
;

UPDATE DBS
  SET DB_LOCATION_URI = CONCAT('s3a', SUBSTR(DB_LOCATION_URI, 4, LENGTH(DB_LOCATION_URI)))
WHERE
      DB_LOCATION_URI LIKE 's3n://%'
;
