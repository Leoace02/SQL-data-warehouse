/*
=================================================================================
Stored procedure: Load Silver Layer (From bronze to silver)
=================================================================================
Script purpose:
  This stored procedure performs the ETL process to populate the 
  'silver' schema tables from the 'bronze' schema.
Actions performed:
  - Truncate Silver Tables
  - Inserts transformed and clean data from the bronze into silver tables
Parameters:
  None
  This stored procedure does not accept any parameters ot returns any values.
Usage Example:
  EXEC silver.load_silver;
=================================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
			SET @batch_start_time= GETDATE();
			PRINT '==============================================';
			PRINT 'Loading SILVER Layer';
			PRINT '==============================================';

			Print '----------------------------------------------';
			PRINT 'Loading CRM Tables';
			Print '----------------------------------------------';
		-- Loading silver.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		Insert into silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
			)
		select 
			cst_id,
			cst_key,
			Trim(cst_firstname)as cst_firstname,
			trim(cst_lastname)as cst_lastname,
			case 
				when upper(trim(cst_marital_status))= 'S' then 'Single'
				when upper(trim(cst_marital_status))= 'M' then 'Married'
				Else 'NA'	
			end as cst_marital_status,   -- Normalizing marital status
			case
				when upper(trim(cst_gndr)) = 'F' then 'Female'
				when upper(trim(cst_gndr)) = 'M' then 'Male'
				Else 'NA'
			End as cst_gndr,			-- Normalizing genders
			cst_create_date
		from(
			select *,
			ROW_Number() OVER (PARTITION BY cst_id Order by cst_create_date DESC) AS flag_last
			from bronze.crm_cust_info
			where cst_id IS NOT NULL 
			)t
			WHERE flag_last =1;          -- Select the most recent record per customer
			SET @end_time = GETDATE();
			PRINT '>> Load Duration:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ 'seconds';
			PRINT '>> ---------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';	
		Insert into silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt 
			 )
			select
			prd_id,
			Replace(SUBSTRING(prd_key, 1, 5 ),'-','_')as cat_id,  -- Extract a specefic part of a string value
			Substring(prd_key,7,LEN(prd_key)) AS prd_key,		  -- Extract product key	
			prd_nm,
			ISNULL(prd_cost, 0) as prd_cost,
			CASE  upper(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'other sales'
			WHEN 'T' THEN 'Touring'
			Else 'n/a'
			End as prd_line,									  -- Map product line codes to descriptive values
			Cast(prd_start_dt as date) as prd_start_dt,
			cast(Lead(prd_start_dt)over (partition by prd_key Order by prd_start_dt)-1 as date) as prd_end_dt  -- Calculate end date as one day before the next start date//(Lead)access values from the next row within a window 
			from bronze.crm_prd_info
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ 'seconds';
		PRINT '>> ---------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
		 sls_ord_num,
		 sls_prd_key,
		 sls_cust_id,
		 sls_order_dt ,
		 sls_ship_dt ,
		 sls_due_dt,
		 sls_sales,
		 sls_quantity ,
		 sls_price 
		)

		Select
		 sls_ord_num ,
		 sls_prd_key ,
		 sls_cust_id ,
		 case when sls_order_dt = 0 or len(sls_order_dt) != 8 Then null
			  else cast(cast(sls_order_dt as VARcHAR) as date)
		 end as sls_order_dt,   
		  case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 Then null
			  else cast(cast(sls_ship_dt as VARcHAR) as date)
		 end as sls_due_dt, 
		   case when sls_due_dt = 0 or len(sls_due_dt) != 8 Then null
			  else cast(cast(sls_due_dt as VARcHAR) as date)
		 end as sls_due_dt, 
		 CASE WHEN sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
			then  sls_quantity * ABS(sls_price) 
			else sls_sales
			end as sls_sales,
		CASE WHEN sls_price is null or sls_price<= 0
			then sls_sales/ nullif(sls_quantity, 0)
			else sls_price
			end as sls_price,
		 sls_quantity
		FROM bronze.crm_sales_details
		SET @end_time = GETDATE();
			PRINT '>> Load Duration:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ 'seconds';
			PRINT '>> ---------------';


			Print '----------------------------------------------';
			PRINT 'Loading ERP Tables';
			Print '----------------------------------------------';	
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT into silver.erp_cust_az12(
		cid,
		bdate,
		gen)
		select 
		case when cid like 'NAS%' then substring(cid,4,len(cid))
		 else cid
		 end cid,
		case when bdate> getdate() then null
		else bdate
		end bdate,
		case when upper(trim(gen)) in ('F','FEMALE') THEN 'Female'
			 when upper(trim(gen)) in ('M','MALE') THEN 'Male'
			 ELSE 'NA'
			 End as gen
		from bronze.erp_cust_az12
		SET @end_time = GETDATE();
			PRINT '>> Load Duration:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ 'seconds';
			PRINT '>> ---------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT into silver.erp_loc_a101(
		cid,cntry)
		select 
		replace (cid,'-','')cid,
		case when trim(cntry)= 'DE' THEN 'Germany'
			 when trim(cntry)in ('US','USA') THEN 'United States'
			 when trim(cntry)= '' OR cntry is NULL THEN 'NA'
			 else trim(cntry)
			 end as cntry
		from bronze.erp_loc_a101;
			SET @end_time = GETDATE();
			PRINT '>> Load Duration:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ 'seconds';
			PRINT '>> ---------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT into silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
		select 
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ 'seconds';
		PRINT '>> ---------------';
				SET @batch_end_time= GETDATE();
			PRINT '=======================';
			PRINT 'Loading Silver Layer is Completed';
			Print '  - Total Load Duration '+ CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time)AS NVARCHAR)+ 'seconds';
			PRINT '=======================';
	END TRY
	BEGIN CATCH --It runs the try block, and if it fails, it runs the catch block to handle the error
		PRINT '==========================================';
		PRINT 'Error Occured During Loading Silver Layer';
		PRINT 'Error Message'+ ERROR_MESSAGE();
		PRINT 'Error Message'+ CAST (ERROR_NUMBER()AS NVARCHAR);
		PRINT 'Error Message'+ CAST (ERROR_STATE()AS NVARCHAR);
		PRINT '==========================================';
	END CATCH
END

EXEC silver.load_silver
