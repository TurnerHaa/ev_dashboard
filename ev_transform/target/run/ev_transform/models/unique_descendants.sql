
  
    

  create  table "postgres"."public"."unique_descendants__dbt_tmp"
  
  
    as
  
  (
    

SELECT DISTINCT descendant_ons 
FROM "postgres"."public"."region_closure"
  );
  