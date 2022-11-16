drop table EDW_DEV.BDV.products;
create table EDW_DEV.BDV.products (
	product_id varchar,
	product_name varchar,
	contains_gluten varchar,
	date_first_sold varchar
);



INSERT INTO EDW_DEV.BDV.products ( product_id, product_name, contains_gluten, date_first_sold ) values 
('1', 'prod_1','True', '2017-02-24'),
('2A', 'prod_2','True', '2022-01-01'),
('3', null,'True', '2022-01-01');


drop table EDW_DEV.BDV.products_final;
create table EDW_DEV.BDV.products_final (
	product_id numeric,
	product_name varchar,
	contains_gluten boolean,
	date_first_sold date
);

drop table EDW_DEV.BDV.DQ_RULE_CONFIG;
create table EDW_DEV.BDV.DQ_RULE_CONFIG (
	src_tbl_name varchar,
	rule_name varchar,
	rule_param varchar
);


INSERT INTO EDW_DEV.BDV.DQ_RULE_CONFIG ( src_tbl_name, rule_name, rule_param) values 
('EDW_DEV.BDV.PRODUCTS', 'RULE_INT','{"COL":"product_id"}'),
('EDW_DEV.BDV.PRODUCTS', 'RULE_NOTNULL','{"COL":"product_name"}')
;



drop table EDW_DEV.BDV.DQ_RULE_VALIDATION_RESULTS ;
create table EDW_DEV.BDV.DQ_RULE_VALIDATION_RESULTS 
(
table_name varchar,
col_name varchar,
invalid_value varchar,
DQ_RULE varchar,
err_msg varchar,
err_rec OBJECT
);
   

CREATE OR REPLACE PROCEDURE EDW_DEV.BDV.DATA_QUALITY_RULE_VALIDATION("SRC_TABLE" VARCHAR(16777216), "DEST_TABLE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
	var value_array = [];
	
    try {
	
		var drp_sql = `drop table IF EXISTS ${SRC_TABLE}_temp`;
		value_array.push(drp_sql);
		rs = snowflake.execute({sqlText: drp_sql});

        var dupchk = `call EDW_DEV.BDV.RULE_ISDUPLICATE(''${SRC_TABLE}'',true)`
        value_array.push(dupchk);
        rs = snowflake.execute({sqlText: dupchk});
        rs.next();

		var rules_sql = `select SRC_TBL_NAME,RULE_NAME,RULE_PARAM from EDW_DEV.BDV.DQ_RULE_CONFIG where UPPER(SRC_TBL_NAME) = UPPER(''${SRC_TABLE}'')`;
        value_array.push(rules_sql);
		var rules_stmt = snowflake.createStatement( {sqlText: rules_sql} );
		var rules_result = rules_stmt.execute(); 
    
	
        while(rules_result.next()) {
            src_tbl = rules_result.getColumnValue(1);
            rule = rules_result.getColumnValue(2);
            param = rules_result.getColumnValue(3);
            

            var cal_sp = `call EDW_DEV.BDV.${rule}(''${src_tbl}'', parse_json(''${param}''), true)`;
            value_array.push(cal_sp);
            rs = snowflake.execute({sqlText: cal_sp});
            rs.next();
        }
        
        var num_rows_sql = `select count(*)  from EDW_DEV.BDV.DQ_RULE_CONFIG where UPPER(SRC_TBL_NAME) = UPPER(''${SRC_TABLE}'')`;
        var stmt = snowflake.createStatement( {sqlText: num_rows_sql} );
        value_array.push(num_rows_sql);
        var row_cnt_out = stmt.execute();
        row_cnt_out.next();
        row_cnt = row_cnt_out.getColumnValue(1)
        value_array.push(row_cnt);
        
        var cols_sql = `call EDW_DEV.BDV.cols_except(''${DEST_TABLE}'','''')`;
        var stmt = snowflake.createStatement( {sqlText: cols_sql} );
        value_array.push(cols_sql);
        var cols_out = stmt.execute();
        cols_out.next();
        cols = cols_out.getColumnValue(1)
        value_array.push(cols);
        
        var merge_sql = ""
        if(row_cnt == 0){
            merge_sql = `insert into ${DEST_TABLE} (${cols}) select ${cols} from (select *, ROW_NUMBER() OVER(PARTITION BY hash(*) order by hash(*) ) as rnm, hash(*) as hashkey from ${SRC_TABLE}_temp) A where A.rnm=1 `; 
        }
        else{
            merge_sql = `insert into ${DEST_TABLE} (${cols}) select ${cols} from (select *, ROW_NUMBER() OVER(PARTITION BY hash(*) order by hash(*) ) as rnm, hash(*) as hashkey from ${SRC_TABLE}_temp) A where A.rnm=(${row_cnt}+1) `;
        }
        

        value_array.push(merge_sql);
        var stmt = snowflake.createStatement( {sqlText: merge_sql} );
        var merge = stmt.execute();
        merge.next();
        rows_inserted = merge.getColumnValue(1)
        value_array.push(rows_inserted);

	}
	catch (err) {
   value_array.push("error found")
   value_array.push(err)
   
    throw value_array.toString();
}
    
    return  rows_inserted;
';



--Stored Proc : 
CREATE OR REPLACE PROCEDURE EDW_DEV.BDV.RULE_INT (TABLE_NAME VARCHAR, CONFIG_NAME VARIANT, REQD_CLEANSE_RECORD BOOLEAN)
RETURNS VARIANT NOT NULL
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var error_msg = [];
    var qry = `
	insert into EDW_DEV.BDV.DQ_RULE_VALIDATION_RESULTS (table_name,col_name,invalid_value,DQ_RULE,err_msg,err_rec)
    SELECT '${TABLE_NAME}',
	    '${CONFIG_NAME["COL"]}',
		${CONFIG_NAME["COL"]},
		'RULE_INT',
        '${CONFIG_NAME["COL"]} is not integer' AS ERR_MSG,
		object_construct(*)
    FROM ${TABLE_NAME}
    WHERE try_to_numeric(${CONFIG_NAME["COL"]}) is  null
	and ${CONFIG_NAME["COL"]} IS NOT NULL
    ;`

	error_msg.push(qry)
    try {
      var rs = snowflake.execute({ sqlText: qry });
	  if(REQD_CLEANSE_RECORD)
	  {
	  var qry = `
	  create temporary table if not exists ${TABLE_NAME}_TEMP
	  like ${TABLE_NAME};`
	  error_msg.push(qry) 
	  var rs_temp = snowflake.execute({ sqlText: qry });
	  
	  var qry = `
	  insert into ${TABLE_NAME}_TEMP
	  select * from ${TABLE_NAME}
	  WHERE try_to_numeric(${CONFIG_NAME["COL"]}) is not  null
	  and ${CONFIG_NAME["COL"]} IS NOT NULL;`
	  error_msg.push(qry)
	  var rs_ins = snowflake.execute({ sqlText: qry });
	  
	  }
	  return error_msg;
    } catch (err) {
        error_msg.push(` {
        sql statement : ‘${qry}’,
          error_code : ‘${err.code}’,
          error_state : ‘${err.state}’,
          error_message : ‘${err.message}’,
          stack_trace : ‘${err.stackTraceTxt}’
          } `);
      return error_msg;		  
    }
$$;

CREATE OR REPLACE PROCEDURE EDW_DEV.BDV.RULE_NOTNULL (TABLE_NAME VARCHAR, CONFIG_NAME VARIANT, REQD_CLEANSE_RECORD BOOLEAN)
RETURNS VARIANT NOT NULL
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var error_msg = [];
    var qry = `
	insert into EDW_DEV.BDV.DQ_RULE_VALIDATION_RESULTS (table_name,col_name,invalid_value,DQ_RULE,err_msg,err_rec)
    SELECT '${TABLE_NAME}',
	    '${CONFIG_NAME["COL"]}',
		${CONFIG_NAME["COL"]},
		'RULE_notnull',
        '${CONFIG_NAME["COL"]} is NULL' AS ERR_MSG,
		object_construct(*)
    FROM ${TABLE_NAME}
    WHERE ${CONFIG_NAME["COL"]} is null
    ;`
	error_msg.push(qry)
    try {
      var rs = snowflake.execute({ sqlText: qry });
	  if(REQD_CLEANSE_RECORD)
	  {
	  var qry = `
	  create temporary table if not exists ${TABLE_NAME}_TEMP
	  like ${TABLE_NAME};`
	  error_msg.push(qry) 
	  var rs_temp = snowflake.execute({ sqlText: qry });
	  
	  var qry = `
	  insert into ${TABLE_NAME}_TEMP
	  select * from ${TABLE_NAME}
	  WHERE ${CONFIG_NAME["COL"]} is not  null;`
	  error_msg.push(qry) 
	  var rs_ins = snowflake.execute({ sqlText: qry });
	  
	  }
	  return error_msg;
    } catch (err) {
        error_msg.push(` {
        sql statement : ‘${qry}’,
          error_code : ‘${err.code}’,
          error_state : ‘${err.state}’,
          error_message : ‘${err.message}’,
          stack_trace : ‘${err.stackTraceTxt}’
          } `);
      return error_msg;		  
    }
$$;



CREATE OR REPLACE PROCEDURE EDW_DEV.BDV.RULE_LENGTH("TABLE_NAME" VARCHAR(16777216), "CONFIG_NAME" VARIANT, "REQD_CLEANSE_RECORD" BOOLEAN)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var error_msg = [];
    var qry = `
	insert into EDW_DEV.BDV.DQ_RULE_VALIDATION_RESULTS (table_name,col_name,invalid_value,DQ_RULE,err_msg,err_rec)
    SELECT ''${TABLE_NAME}'',
	    ''${CONFIG_NAME["COL"]}'',
		${CONFIG_NAME["COL"]},
		''RULE_LENGTH'',
        ''${CONFIG_NAME["COL"]} has more length '' AS ERR_MSG,
		object_construct(*)
    FROM ${TABLE_NAME}
    WHERE length(${CONFIG_NAME["COL"]}) >  ${CONFIG_NAME["LENGTH"]} AND ${CONFIG_NAME["COL"]} IS NOT NULL
    ;`
	error_msg.push(qry)
    try {
      var rs = snowflake.execute({ sqlText: qry });
	  if(REQD_CLEANSE_RECORD)
	  {
	  var qry = `
	  create temporary table if not exists ${TABLE_NAME}_TEMP
	  like ${TABLE_NAME};`
	  error_msg.push(qry) 
	  var rs_temp = snowflake.execute({ sqlText: qry });
	  
	  var qry = `
	  insert into ${TABLE_NAME}_TEMP
	  select * from ${TABLE_NAME}
	  WHERE length(${CONFIG_NAME["COL"]}) <=  ${CONFIG_NAME["LENGTH"]} OR ${CONFIG_NAME["COL"]} IS NULL
	  ;`
	  error_msg.push(qry) 
	  var rs_ins = snowflake.execute({ sqlText: qry });
	  
	  }
	  return error_msg;
    } catch (err) {
        error_msg.push(` {
        sql statement : ‘${qry}’,
          error_code : ‘${err.code}’,
          error_state : ‘${err.state}’,
          error_message : ‘${err.message}’,
          stack_trace : ‘${err.stackTraceTxt}’
          } `);
      return error_msg;		  
    }
';

CREATE OR REPLACE PROCEDURE EDW_DEV.BDV.RULE_ISTIMESTAMP("TABLE_NAME" VARCHAR(16777216), "CONFIG_NAME" VARIANT, "REQD_CLEANSE_RECORD" BOOLEAN)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var error_msg = [];
    var qry = `
	insert into EDW_DEV.BDV.DQ_RULE_VALIDATION_RESULTS (table_name,col_name,invalid_value,DQ_RULE,err_msg,err_rec)
    SELECT ''${TABLE_NAME}'',
	    ''${CONFIG_NAME["COL"]}'',
		${CONFIG_NAME["COL"]},
		''RULE_ISTIMESTAMP'',
        ''${CONFIG_NAME["COL"]} is NOT HAVING THE CORRCT FORMAT'' AS ERR_MSG,
		object_construct(*)
    FROM ${TABLE_NAME}
    WHERE try_to_timestamp(${CONFIG_NAME["COL"]}) IS NULL
    AND ${CONFIG_NAME["COL"]} IS NOT NULL
    ;`
	error_msg.push(qry)
    try {
      var rs = snowflake.execute({ sqlText: qry });
	  if(REQD_CLEANSE_RECORD)
	  {
	  var qry = `
	  create temporary table if not exists ${TABLE_NAME}_TEMP
	  like ${TABLE_NAME};`
	  error_msg.push(qry) 
	  var rs_temp = snowflake.execute({ sqlText: qry });
	  
	  var qry = `
	  insert into ${TABLE_NAME}_TEMP
	  select * from ${TABLE_NAME}
	  WHERE try_to_timestamp(${CONFIG_NAME["COL"]}) IS NOT NULL
      OR ${CONFIG_NAME["COL"]} IS NULL
    ;`
	  error_msg.push(qry) 
	  var rs_ins = snowflake.execute({ sqlText: qry });
	  
	  }
	  return error_msg;
    } catch (err) {
        error_msg.push(` {
        sql statement : ‘${qry}’,
          error_code : ‘${err.code}’,
          error_state : ‘${err.state}’,
          error_message : ‘${err.message}’,
          stack_trace : ‘${err.stackTraceTxt}’
          } `);
      return error_msg;		  
    }
';

CREATE OR REPLACE PROCEDURE EDW_DEV.BDV.RULE_ISTIME("TABLE_NAME" VARCHAR(16777216), "CONFIG_NAME" VARIANT, "REQD_CLEANSE_RECORD" BOOLEAN)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var error_msg = [];
    var qry = `
	insert into EDW_DEV.BDV.DQ_RULE_VALIDATION_RESULTS (table_name,col_name,invalid_value,DQ_RULE,err_msg,err_rec)
    SELECT ''${TABLE_NAME}'',
	    ''${CONFIG_NAME["COL"]}'',
		${CONFIG_NAME["COL"]},
		''RULE_ISTIME'',
        ''${CONFIG_NAME["COL"]} is NOT HAVING THE CORRCT FORMAT'' AS ERR_MSG,
		object_construct(*)
    FROM ${TABLE_NAME}
    WHERE try_to_time(${CONFIG_NAME["COL"]}) IS NULL
    AND ${CONFIG_NAME["COL"]} IS NOT NULL
    ;`
	error_msg.push(qry)
    try {
      var rs = snowflake.execute({ sqlText: qry });
	  if(REQD_CLEANSE_RECORD)
	  {
	  var qry = `
	  create temporary table if not exists ${TABLE_NAME}_TEMP
	  like ${TABLE_NAME};`
	  error_msg.push(qry) 
	  var rs_temp = snowflake.execute({ sqlText: qry });
	  
	  var qry = `
	  insert into ${TABLE_NAME}_TEMP
	  select * from ${TABLE_NAME}
	  WHERE try_to_time(${CONFIG_NAME["COL"]}) IS NOT NULL
      OR ${CONFIG_NAME["COL"]} IS NULL
    ;`
	  error_msg.push(qry) 
	  var rs_ins = snowflake.execute({ sqlText: qry });
	  
	  }
	  return error_msg;
    } catch (err) {
        error_msg.push(` {
        sql statement : ‘${qry}’,
          error_code : ‘${err.code}’,
          error_state : ‘${err.state}’,
          error_message : ‘${err.message}’,
          stack_trace : ‘${err.stackTraceTxt}’
          } `);
      return error_msg;		  
    }
';



CREATE OR REPLACE PROCEDURE EDW_DEV.BDV.RULE_ISDUPLICATE("TABLE_NAME" VARCHAR(16777216), "REQD_CLEANSE_RECORD" BOOLEAN)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var error_msg = [];
    var qry = `
	insert into EDW_DEV.BDV.DQ_RULE_VALIDATION_RESULTS (table_name,col_name,invalid_value,DQ_RULE,err_msg,err_rec)
    SELECT ''${TABLE_NAME}'',
	    '''',
		'''',
		''RULE_ISDUPLICATE'',
        ''ROW is a duplicate'' AS ERR_MSG,
		object_construct(A.*)
    FROM (select *, ROW_NUMBER() OVER(PARTITION BY hash(*) order by hash(*) ) as rnm, hash(*) as hashkey from ${TABLE_NAME}) A
    where A.rnm > 1
    ;`
	error_msg.push(qry)
    try {
      var rs = snowflake.execute({ sqlText: qry });
	  if(REQD_CLEANSE_RECORD)
	  {
      
	  var qry = `
	  create temporary table if not exists ${TABLE_NAME}_TEMP
	  like ${TABLE_NAME};`
	  error_msg.push(qry) 
	  var rs_temp = snowflake.execute({ sqlText: qry });
      
      var cols_sql = `call EDW_DEV.BDV.cols_except(''${TABLE_NAME}_TEMP'','''')`;
      var stmt = snowflake.createStatement( {sqlText: cols_sql} );
      error_msg.push(cols_sql);
      var cols_out = stmt.execute();
      cols_out.next();
      cols = cols_out.getColumnValue(1)
      error_msg.push(cols);
	  
	  var qry = `
	  insert into ${TABLE_NAME}_TEMP
	  select ${cols} from 
	  (select *, ROW_NUMBER() OVER(PARTITION BY hash(*) order by hash(*) ) as rnm, hash(*) as hashkey from ${TABLE_NAME}) A
	  where A.rnm = 1
    ;`
	  error_msg.push(qry) 
	  var rs_ins = snowflake.execute({ sqlText: qry });
	  
	  }
	  return error_msg;
    } catch (err) {
        error_msg.push(` {
        sql statement : ‘${qry}’,
          error_code : ‘${err.code}’,
          error_state : ‘${err.state}’,
          error_message : ‘${err.message}’,
          stack_trace : ‘${err.stackTraceTxt}’
          } `);
      return error_msg;		  
    }
';


CREATE OR REPLACE PROCEDURE EDW_DEV.BDV.COLS_EXCEPT("TABLE_NAME" VARCHAR(16777216), "EXCEPT" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS 'begin
    describe table identifier(:table_name);
    return (
        select listagg("name", '', '') cols_except
        from table(result_scan(last_query_id())) 
        where not array_contains("name"::variant, (split(:except, '',''))) 
    );
end';



select * from EDW_DEV.BDV.products;
select * from EDW_DEV.BDV.products_final;
select * from EDW_DEV.BDV.DQ_RULE_CONFIG;

select * from EDW_DEV.BDV.DQ_RULE_CONFIG;


select * from EDW_DEV.BDV.DQ_RULE_VALIDATION_RESULTS ;

call EDW_DEV.BDV.DATA_QUALITY_RULE_VALIDATION('EDW_DEV.BDV.products','EDW_DEV.BDV.products_final')

call BDV.RULE_ISTIME('vardttm',parse_json('{"COL":"v","FUNC":"is_date"}'),FALSE)
