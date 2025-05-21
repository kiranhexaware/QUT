CREATE OR REPLACE PACKAGE SDB_FROM_CCR AS

  PROCEDURE Main_Control;
  
END SDB_FROM_CCR;
/


CREATE OR REPLACE PACKAGE BODY SDB_FROM_CCR AS
  gc_interface_name  CONSTANT VARCHAR2 (20) := 'SDB_FROM_CCR';
  g_run_start        TIMESTAMP;
  g_run_end          TIMESTAMP;
 
  e_TRANS_FAIL       EXCEPTION;  -- Raised by transactions

  ----------------------------------------------------------------------------
  -- Original stored procedure from Corella
  ----------------------------------------------------------------------------
  
  PROCEDURE load_ccr_missing  IS
  --
  --  Procedure to create records for all CCR people who are missing from the
  --  in the sdb_client_lookup and sdb_clients table
  --
  --  Amendment History
  --  Version 1.0  3 Jan 2003  J Chapman
  --
  --  10 Feb 2011  D Peterson   Migrated into OMG Hub
  --                            Still needs various SDB updates replaced with 
  --                            standard upsert procedure calls

  c_trans_name CONSTANT varchar2(60) := 'load_ccr_missing';    

  CURSOR cCCR IS
  SELECT ip.employee_num, 
         ip.ip_num, 
         cc.ccr_client_id,
         cc.surname, 
         cc.first_name, 
         cc.second_name, 
         cc.preferred_name,
         cc.title, 
         cc.birth_date
  FROM   ccr_clients      cc
        ,ip ip
        ,ccr_client_roles ccr
  WHERE (ip.ip_num,ccr.start_date, ccr.end_date) IN
       (SELECT ip.ip_num, MIN(ccr.start_date),MAX(ccr.end_date)
        FROM   ip ip
              ,ccr_clients cc
              ,ccr_client_roles ccr
        WHERE  ip.ip_type IN ('CCR','OTH')
        AND    ip.ip_status IN ('cur','fut')
        AND    cc.ip_num     = ip.ip_num
        AND    cc.start_date =
             (SELECT MAX(start_date)
              FROM   ccr_clients
              WHERE  ccr_client_id = cc.ccr_client_id)
        AND    ccr.client_id = cc.ccr_client_id
        AND    ccr.end_date >= SYSDATE + 1
        GROUP BY ip.ip_num
        MINUS
        SELECT ccr_id, ca.start_date, ca.end_date
        FROM   sdb_client_lookup cl
              ,sdb_client_attributes ca
        WHERE  cl.ccr_id IS NOT NULL
        AND    ca.client_id = cl.client_id
        AND    ca.attr_type = 'ROLE'
        AND    ca.attr_id   = 'CCR')
  AND   cc.ip_num = ip.ip_num
  AND   cc.start_date =
             (SELECT MAX(start_date)
              FROM   ccr_clients
              WHERE  ccr_client_id = cc.ccr_client_id)
  AND   ccr.client_id = cc.ccr_client_id
  AND   ccr.end_date >= SYSDATE + 1;

  l_attr_data               VARCHAR2(80);
  l_attr_id                 VARCHAR2(10);
  l_client_id               VARCHAR2(10);
  l_client_lookup           sdb_client_lookup%ROWTYPE;
  l_continue                BOOLEAN;
  l_count                   NUMBER(10);
  l_count_attr_no_change    NUMBER(10);
  l_count_attr_insert       NUMBER(10);
  l_count_attr_update       NUMBER(10);
  l_count_client_no_change  NUMBER(10);
  l_count_client_insert     NUMBER(10);
  l_count_client_update     NUMBER(10);

  l_count_no_client         NUMBER(10);
  l_count_insert            NUMBER(10);
  l_count_lookup_update     NUMBER(10);
  l_count_new_lookup        NUMBER(10);
  l_count_lookup_exists     NUMBER(10);
  l_count_update            NUMBER(10);
  l_current_attribute       sdb_client_attributes%ROWTYPE;
  l_current_client          sdb_clients%ROWTYPE;
  l_dummy                   VARCHAR2(10);
  l_end_date                DATE;
  l_full_name               VARCHAR2(200);
  l_found                   BOOLEAN;
  l_start_date              DATE;
  
  l_null_dt                 date := TO_DATE('01-JAN-1800','DD-MON-YYYY');

  BEGIN
    l_count                   := 0;
    l_count_insert            := 0;
    l_count_update            := 0;
    l_count_lookup_exists     := 0;
    l_count_lookup_update     := 0;
    l_count_new_lookup        := 0;
    l_count_client_no_change  := 0;
    l_count_client_update     := 0;
    l_count_client_insert     := 0;
    l_count_attr_no_change    := 0;
    l_count_attr_update       := 0;
    l_count_attr_insert       := 0;

    FOR CCRRec IN cCCR LOOP
      l_count := l_count + 1;
      IF  CCRRec.preferred_name IS NULL THEN
          l_full_name := CCRRec.preferred_name || ' ' || CCRRec.surname;
      ELSE
          l_full_name := CCRRec.first_name     || ' ' || CCRRec.surname;
      END IF;

      --  Check that this employee is in sdb_client_lookup
      --  First, query by CCR ID
      --
      BEGIN
          SELECT client_id
          INTO   l_client_id
          FROM   sdb_client_lookup
          WHERE  CCR_id = TO_CHAR(CCRRec.ip_num);
          l_found := TRUE;
      EXCEPTION
          WHEN NO_DATA_FOUND THEN
              l_found := FALSE;
      END;
              
              
      --
      -- If the CCR ID is not in the table
      -- look for a client ID that matches (such as a former employee)
      --
      IF  NOT l_found THEN
        l_client_id := TO_CHAR(CCRRec.ip_num);
        BEGIN
            SELECT *
            INTO   l_client_lookup
            FROM   sdb_client_lookup
            WHERE  client_id = TO_CHAR(CCRRec.ip_num);
            l_found := TRUE;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                l_found := FALSE;
        END;
              
        IF  l_found THEN
        --  Found a record
        --  then update the CCR ID
        --
          IF  l_client_lookup.ccr_id IS NULL AND
            CCRRec.ip_num IS NOT NULL THEN
            BEGIN
              SAVEPOINT scl_update;
                          
              UPDATE sdb_client_lookup
              SET    ccr_id = CCRRec.ip_num
              WHERE  client_id = l_client_id;
                          
                l_count_lookup_update := l_count_lookup_update + 1;
            EXCEPTION
              WHEN OTHERS THEN
                 --DBMS_OUTPUT.PUT_LINE('Error processing SCL_Update for client_id/ip_num '||l_client_id||'/'||CCRRec.ip_num);
                 HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'ERROR', 'SDB_CLIENT_LOOKUP',
                                    'client_id/ip_num '||l_client_id||'/'||CCRRec.ip_num, 
                                    'Error processing SCL_Update for client_id/ip_num '||
                                    l_client_id||'/'||CCRRec.ip_num||
                                    ' - '||SQLERRM);
                 ROLLBACK TO scl_update;     
            END;
          END IF;
        ELSE   --  No record found in sdb_client_lookup
          BEGIN
            INSERT
            INTO    sdb_client_lookup
                   (client_id, CCR_id)
            VALUES (l_client_id,CCRRec.ip_num);
            l_count_new_lookup := l_count_new_lookup + 1;
          END;
        END IF;
      ELSE
          l_count_lookup_exists := l_count_lookup_exists + 1;
      END IF;
          --  Check that this employee is in sdb_clients
      BEGIN
          SELECT *
          INTO   l_current_client
          FROM   sdb_clients
          WHERE  client_id = l_client_id;
          l_found := TRUE;
      EXCEPTION
          WHEN NO_DATA_FOUND THEN
              l_found := FALSE;
      END;
          
      IF  l_found THEN
        IF  NVL(l_current_client.surname,'NULL')                 != NVL(CCRRec.surname,'NULL')
            OR NVL(l_current_client.first_name,'NULL')           != NVL(CCRRec.first_name,'NULL')
            OR NVL(l_current_client.second_name,'NULL')          != NVL(CCRRec.second_name,'NULL')
            OR NVL(l_current_client.preferred_first_name,'NULL') != NVL(CCRRec.preferred_name,'NULL')
            OR NVL(l_current_client.title,'NULL') != NVL(CCRRec.title,'NULL')
            OR (l_current_client.birth_date IS NOT NULL
                AND CCRRec.birth_date IS NOT NULL
                AND l_current_client.birth_date != CCRRec.birth_date)
            OR (l_current_client.birth_date IS NOT NULL
                AND CCRRec.birth_date IS NULL )
            OR (l_current_client.birth_date IS NULL
                AND CCRRec.birth_date IS NOT NULL ) THEN

          BEGIN
            UPDATE  sdb_clients
            SET     first_name  = CCRRec.first_name
                   ,second_name = CCRRec.second_name
                      ,surname     = CCRRec.surname
                      ,title       = CCRRec.title
                      ,preferred_first_name = CCRRec.preferred_name
                      ,birth_date   = CCRRec.birth_date
                      ,full_name    = l_full_name
                      ,update_who   = 'HUB_LINK'
                      ,update_on    = SYSDATE
            WHERE   client_id = l_client_id;
            l_count_client_update := l_count_client_update + 1;
          END;
        ELSE
          l_count_client_no_change := l_count_client_no_change + 1;
        END IF;
      ELSE  -- Not in sdb_clients
        BEGIN
          INSERT
          INTO    sdb_clients
                 (client_id, first_name, second_name, surname, preferred_first_name,
                  full_name, title, birth_date, update_who, update_on)
          VALUES (l_client_id
                 ,CCRRec.first_name
                 ,CCRRec.second_name
                 ,CCRRec.surname
                 ,CCRRec.preferred_name
                 ,l_full_name
                 ,CCRRec.title
                 ,CCRRec.birth_date
                 ,'HUB_LINK'
                 ,SYSDATE
                 );
          l_count_client_insert := l_count_client_insert + 1;
        END;
      END IF;
        
      --  Check that this employee is in sdb_client_attributes
      --  with a CCR ROLE
      BEGIN
        SELECT *
        INTO   l_current_attribute
        FROM   sdb_client_attributes
        WHERE  client_id = l_client_id
        AND    attr_type = 'ROLE'
        AND    attr_id   = 'CCR';
        l_found := TRUE;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
            l_found := FALSE;
      END;
        
      BEGIN
        SELECT MIN(start_date)
          INTO l_start_date
          FROM ccr_client_roles
         WHERE client_id = CCRRec.ccr_client_id;
      END;
      BEGIN
        SELECT MAX(end_date)
          INTO l_end_date
          FROM ccr_client_roles
         WHERE client_id = CCRRec.ccr_client_id;
      END;
      l_attr_data := LPAD(TO_CHAR(CCRRec.ccr_client_id),8,'0');
      IF  l_found THEN
        IF  NVL(l_current_attribute.attr_data,'NULL') != l_attr_data OR
            NVL(l_current_attribute.start_date,l_null_dt) !=
            NVL(l_start_date, l_null_dt) OR
            NVL(l_current_attribute.end_date, l_null_dt)   !=
            NVL(l_end_date, l_null_dt)  THEN
          BEGIN
            UPDATE  sdb_client_attributes
            SET     attr_data    = l_attr_data
                   ,start_date   = l_start_date
                      ,end_date     = l_end_date
                      ,update_who   = 'HUB_LINK'
                      ,update_on    = SYSDATE
            WHERE   client_id    = l_client_id
            AND     attr_type    = 'ROLE'
            AND     attr_id      = 'CCR';
            l_count_attr_update := l_count_attr_update + 1;
          END;
        ELSE
          l_count_attr_no_change := l_count_attr_no_change + 1;
        END IF;
      ELSE
        BEGIN
          INSERT
          INTO    sdb_client_attributes
                 (client_id, attr_type, attr_id, attr_data,
                  start_date, end_date, update_who, update_on)
          VALUES (l_client_id
                 ,'ROLE'
                 ,'CCR'
                 ,l_attr_data
                 ,l_start_date
                 ,l_end_date
                 ,'HUB_LINK'
                 ,SYSDATE
                 );
          l_count_attr_insert := l_count_attr_insert + 1;
        END;
      END IF;

      IF  MOD(l_count, 1000) = 0 THEN
          COMMIT;
      END IF;
      END LOOP;
      COMMIT;
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, NULL, 
                         'Found    '     || TO_CHAR(l_count) || ' records in CCR.');
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, NULL, 
                         'Found    '     || TO_CHAR(l_count_lookup_exists) || ' records already in sdb_client_lookup.');
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, NULL, 
                         'Updated  '     || TO_CHAR(l_count_lookup_update) || ' records in sdb_client_lookup.');
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, NULL, 
                         'Added    '     || TO_CHAR(l_count_new_lookup) || ' records to sdb_client_lookup.');
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, NULL, 
                         'Found    '     || TO_CHAR(l_count_client_no_change) || ' unchanged records in sdb_clients.');
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, NULL, 
                         'Updated  '     || TO_CHAR(l_count_client_update) || ' records in sdb_clients.');
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, NULL, 
                         'Added    '     || TO_CHAR(l_count_client_insert) || ' records to sdb_clients.');
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, NULL, 
                         'Found    '     || TO_CHAR(l_count_attr_no_change) || ' unchanged records in sdb_client_attributes.');
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, NULL, 
                         'Updated  '     || TO_CHAR(l_count_attr_update) || ' records in sdb_client_attributes.');
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, NULL, 
                         'Added    '     || TO_CHAR(l_count_attr_insert) || ' records to sdb_client_attributes.');

  END load_ccr_missing;




  PROCEDURE Main_Control IS
    c_this_proc      CONSTANT VARCHAR2(20) := 'MAIN_CONTROL';
    l_start_time     TIMESTAMP;
    l_end_time       TIMESTAMP;
    l_elapsed_time   INTERVAL DAY (2) TO SECOND (6);    
    
  BEGIN                
    -- Log that this interface has started.
    l_start_time := LOCALTIMESTAMP;
    HUB_LOG.LOG_WRITE (gc_interface_name, c_this_proc, 'INFO', NULL, 
                       'Starting ' || gc_interface_name, 
                       'START AT: ' || TO_CHAR (l_start_time));
    BEGIN
    
        Load_CCR_Missing; -- Old QHO stored procedure
    EXCEPTION
        -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.        
        WHEN OTHERS THEN
        HUB_LOG.LOG_WRITE (gc_interface_name, c_this_proc, 'ERROR', NULL, NULL, 
                           'MAIN_CONTROL WHEN OTHERS Exception '||' - '||SQLERRM);
    END;

    -- Log that this interface has finished.
    l_end_time := LOCALTIMESTAMP;
    l_elapsed_time := l_end_time - l_start_time;
    HUB_LOG.LOG_WRITE (gc_interface_name, c_this_proc, 'INFO', NULL, 
                       'Elapsed TIME ' || l_elapsed_time, 
                       'Ended AT: ' || TO_CHAR (l_end_time));
  END;
  
END SDB_FROM_CCR;
/
