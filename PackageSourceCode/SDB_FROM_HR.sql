CREATE OR REPLACE PACKAGE SDB_FROM_HR AS

  PROCEDURE MAIN_CONTROL;

END SDB_FROM_HR;
/


CREATE OR REPLACE PACKAGE BODY SDB_FROM_HR
AS

  gc_interface_name  CONSTANT VARCHAR2 (20) := 'SDB_from_HR';
  g_run_start        TIMESTAMP;
  g_run_end          TIMESTAMP;
  -- Indicator if any failures encountered
  g_failed_trans     BOOLEAN                := FALSE;

  e_RECORD_FAIL      EXCEPTION;  -- raised by Upsert procedures
  
  e_TRANS_FAIL       EXCEPTION;  -- Raised by transactions
   

   PROCEDURE OFFSET_RUN_TS (P_TS IN OUT TIMESTAMP) 
   IS
    L_RUN_OFFSET_NAME           VARCHAR2(20) := 'OFFSET_INTERVAL';
    L_RUN_OFFSET_VALUE          VARCHAR2(20);
    
   BEGIN
      -- Offset run-start-timestamp to account for timezone difference
      -- between HUB and AscenderPay
      HUB_LIB.GET_HUB_VALUE(GC_INTERFACE_NAME, L_RUN_OFFSET_NAME, L_RUN_OFFSET_VALUE);
      
      P_TS := P_TS + TO_DSINTERVAL(L_RUN_OFFSET_VALUE);
      
   END OFFSET_RUN_TS;
  
  
  ----------------------------------------------------------------------------
  -- Internal Upsert and Delete procedures for the various SDB tables
  ----------------------------------------------------------------------------

  PROCEDURE Set_SDB_ORG_UNITS
               (p_ou_rec IN sdb_org_units%ROWTYPE,
                p_caller  IN VARCHAR2 DEFAULT NULL) IS
    -- Upsert a single record into the SDB_ORG_UNITS table
    c_trans_name CONSTANT VARCHAR2(60) := NVL(p_caller, 
                                                    'Set_SDB_ORG_UNITS');
    l_phase       VARCHAR2(100) := 'Initialising';
    l_key_data    VARCHAR2(100) := 'ORG_UNIT: '||p_ou_rec.org_unit;

    l_ou_rec      sdb_org_units%ROWTYPE := NULL;
  BEGIN
    l_phase := 'Attempting SELECT - '||l_key_data;
    SELECT *
      INTO l_ou_rec
      FROM sdb_org_units
     WHERE org_unit = p_ou_rec.org_unit;
    
    l_phase := 'Attempting UPDATE - '||l_key_data;
    UPDATE sdb_org_units
       SET start_date   = p_ou_rec.start_date
          ,end_date     = p_ou_rec.end_date
          ,description  = p_ou_rec.description
          ,own_org_unit = p_ou_rec.own_org_unit
     WHERE org_unit = p_ou_rec.org_unit;
       
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      BEGIN
         l_phase := 'Attempting INSERT - '||l_key_data;
         INSERT INTO sdb_org_units
               (org_unit
               ,start_date
               ,end_date    
               ,description
               ,own_org_unit)
         VALUES (p_ou_rec.org_unit
                ,p_ou_rec.start_date
                ,p_ou_rec.end_date
                ,p_ou_rec.description
                ,p_ou_rec.own_org_unit);    
      EXCEPTION
         WHEN OTHERS THEN
            HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 
                               'ERROR', 'SDB_ORG_UNITS',
                               l_key_data, l_phase||' - '||SQLERRM);
            RAISE e_RECORD_FAIL;
      END;
    WHEN OTHERS THEN
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 
                         'ERROR', 'SDB_ORG_UNITS',
                         l_key_data, l_phase||' - '||SQLERRM);
      RAISE e_RECORD_FAIL;
  END Set_SDB_ORG_UNITS;
  

  PROCEDURE Set_SDB_CLIENTS
              (p_client_rec IN SDB_CLIENTS%ROWTYPE,
                p_caller  IN VARCHAR2 DEFAULT NULL) IS
    -- Upsert a single record into the SDB_CLIENTS table
    c_trans_name CONSTANT VARCHAR2(60) := NVL(p_caller, 
                                                    'Set_SDB_CLIENTS');    
    l_phase       VARCHAR2(100) := 'Initialising';
    l_key_data    VARCHAR2(100) := 'CLIENT_ID: '||p_client_rec.client_id;

    l_client_rec  SDB_CLIENTS%ROWTYPE := NULL;
  BEGIN
    l_phase := 'Attempting SELECT - '||l_key_data;
    SELECT *
      INTO l_client_rec
      FROM sdb_clients
     WHERE client_id = p_client_rec.client_id;
    
    l_phase := 'Attempting UPDATE - '||l_key_data;
    UPDATE sdb_clients
       SET surname              = p_client_rec.surname
          ,first_name           = p_client_rec.first_name
          ,second_name          = p_client_rec.second_name
          ,preferred_first_name = p_client_rec.preferred_first_name
          ,full_name            = p_client_rec.full_name
          ,title                = p_client_rec.title
          ,birth_date           = p_client_rec.birth_date
     WHERE client_id = p_client_rec.client_id;
       
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      BEGIN
         l_phase := 'Attempting INSERT - '||l_key_data;
         INSERT INTO sdb_clients
               (client_id
               ,surname
               ,first_name
               ,second_name
               ,preferred_first_name
               ,full_name
               ,title
               ,birth_date)
         VALUES (p_client_rec.client_id
                ,p_client_rec.surname
                ,p_client_rec.first_name
                ,p_client_rec.second_name
                ,p_client_rec.preferred_first_name
                ,p_client_rec.full_name
                ,p_client_rec.title
                ,p_client_rec.birth_date);
      EXCEPTION
       WHEN OTHERS THEN
         HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 
                            'ERROR', 'SDB_CLIENTS',
                            l_key_data, l_phase||' - '||SQLERRM);
         RAISE e_RECORD_FAIL;      
      END;
    WHEN OTHERS THEN
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 
                         'ERROR', 'SDB_CLIENTS',
                         l_key_data, l_phase||' - '||SQLERRM);
      RAISE e_RECORD_FAIL;
  END Set_SDB_CLIENTS;
    

  PROCEDURE Set_SDB_CLIENT_ATTRIBUTES
               (p_attr_rec IN SDB_CLIENT_ATTRIBUTES%ROWTYPE,
                p_caller  IN VARCHAR2 DEFAULT NULL) IS
    -- Upsert a single record into the SDB_CLIENT_ATTRIBUTES table
    --  6 Jul 2011  DP  Added error trap around INSERT phase
    c_trans_name CONSTANT VARCHAR2(60) := NVL(p_caller, 
                                                    'Set_SDB_CLIENT_ATTRIBUTES');    
    l_phase       VARCHAR2(100) := 'Initialising';
    l_key_data    VARCHAR2(100) := 'CLIENT_ID/ATTR_TYPE/ATTR_ID: '||
                                    p_attr_rec.client_id||'/'||
                                    p_attr_rec.attr_type||'/'||
                                    p_attr_rec.attr_id;

    l_attr_rec    SDB_CLIENT_ATTRIBUTES%ROWTYPE := NULL;
  BEGIN
    l_phase := 'Attempting SELECT - '||l_key_data;    
    SELECT *
      INTO l_attr_rec
      FROM sdb_client_attributes
     WHERE client_id = p_attr_rec.client_id
       AND attr_type = p_attr_rec.attr_type
       AND attr_id   = p_attr_rec.attr_id;
    
    l_phase := 'Attempting UPDATE - '||l_key_data;
    UPDATE sdb_client_attributes
       SET start_date    = p_attr_rec.start_date
          ,end_date      = p_attr_rec.end_date
          ,attr_data     = p_attr_rec.attr_data
          ,own_client_id = p_attr_rec.own_client_id
          ,own_attr_type = p_attr_rec.own_attr_type
          ,own_attr_id   = p_attr_rec.own_attr_id
     WHERE client_id = p_attr_rec.client_id
       AND attr_type = p_attr_rec.attr_type
       AND attr_id   = p_attr_rec.attr_id;
       
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      BEGIN
        l_phase := 'Attempting INSERT - '||l_key_data;
        INSERT INTO sdb_client_attributes
              (client_id
              ,attr_type
              ,attr_id 
              ,start_date
              ,end_date    
              ,attr_data    
              ,own_client_id
              ,own_attr_type
              ,own_attr_id)
        VALUES (p_attr_rec.client_id
               ,p_attr_rec.attr_type
               ,p_attr_rec.attr_id
               ,p_attr_rec.start_date
               ,p_attr_rec.end_date
               ,p_attr_rec.attr_data    
               ,p_attr_rec.own_client_id
               ,p_attr_rec.own_attr_type
               ,p_attr_rec.own_attr_id);
      EXCEPTION
        WHEN OTHERS THEN
          HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 
                             'ERROR', 'SDB_CLIENT_ATTRIBUTES',
                             l_key_data, l_phase||' - '||SQLERRM);
          RAISE e_RECORD_FAIL;
      END;
    WHEN OTHERS THEN
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 
                         'ERROR', 'SDB_CLIENT_ATTRIBUTES',
                         l_key_data, l_phase||' - '||SQLERRM);
      RAISE e_RECORD_FAIL;
  END Set_SDB_CLIENT_ATTRIBUTES;
  
  
  PROCEDURE Del_SDB_CLIENT_ATTRIBUTES
               (p_attr_rec IN SDB_CLIENT_ATTRIBUTES%ROWTYPE,
                p_caller  IN VARCHAR2 DEFAULT NULL) IS
    -- Delete a single record from the SDB_CLIENT_ATTRIBUTES table
    c_trans_name CONSTANT VARCHAR2(60) := NVL(p_caller, 
                                                    'Del_SDB_CLIENT_ATTRIBUTES');    
    l_phase       VARCHAR2(100) := 'Initialising';
    l_key_data    VARCHAR2(100) := 'CLIENT_ID/ATTR_TYPE/ATTR_ID: '||
                                    p_attr_rec.client_id||'/'||
                                    p_attr_rec.attr_type||'/'||
                                    p_attr_rec.attr_id;
  BEGIN
    l_phase := 'Attempting DELETE - '||l_key_data;
    DELETE
      FROM sdb_client_attributes
     WHERE client_id = p_attr_rec.client_id
       AND attr_type = p_attr_rec.attr_type
       AND attr_id   = p_attr_rec.attr_id;
    
  EXCEPTION
    WHEN OTHERS THEN
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 
                         'ERROR', 'SDB_CLIENT_ATTRIBUTES',
                         l_key_data, l_phase||' - '||SQLERRM);
      RAISE e_RECORD_FAIL;
  END Del_SDB_CLIENT_ATTRIBUTES;
  
  
  PROCEDURE Set_SDB_ATTRIBUTES
               (p_attr_rec IN SDB_ATTRIBUTES%ROWTYPE,
                p_caller  IN VARCHAR2 DEFAULT NULL) IS
    -- Upsert a single record into the SDB_ATTRIBUTES table
    c_trans_name CONSTANT VARCHAR2(60) := NVL(p_caller, 
                                                    'Set_SDB_ATTRIBUTES');    
    l_phase       VARCHAR2(100) := 'Initialising';
    l_key_data    VARCHAR2(100) := 'ATTR_TYPE/ATTR_ID: '||
                                    p_attr_rec.attr_type||'/'||
                                    p_attr_rec.attr_id;

    l_attr_rec    SDB_ATTRIBUTES%ROWTYPE := NULL;
  BEGIN
    l_phase := 'Attempting SELECT - '||l_key_data;    
    SELECT *
      INTO l_attr_rec
      FROM sdb_attributes
     WHERE attr_type = p_attr_rec.attr_type
       AND attr_id   = p_attr_rec.attr_id;
    
    l_phase := 'Attempting UPDATE - '||l_key_data;    
    UPDATE sdb_attributes
       SET start_date  = p_attr_rec.start_date
          ,end_date    = p_attr_rec.end_date
          ,org_unit    = p_attr_rec.org_unit
          ,description = p_attr_rec.description
     WHERE attr_type = p_attr_rec.attr_type
       AND attr_id   = p_attr_rec.attr_id;
       
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      BEGIN
         l_phase := 'Attempting INSERT - '||l_key_data;    
         INSERT INTO sdb_attributes
               (attr_type
               ,attr_id 
               ,start_date
               ,end_date    
               ,org_unit    
               ,description)
         VALUES (p_attr_rec.attr_type
                ,p_attr_rec.attr_id
                ,p_attr_rec.start_date
                ,p_attr_rec.end_date
                ,p_attr_rec.org_unit
                ,p_attr_rec.description);    
      EXCEPTION WHEN OTHERS THEN
         HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 
                            'ERROR', 'SDB_ATTRIBUTES',
                            l_key_data, l_phase||' - '||SQLERRM);
         RAISE e_RECORD_FAIL;      
      END;
    WHEN OTHERS THEN
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 
                         'ERROR', 'SDB_ATTRIBUTES',
                         l_key_data, l_phase||' - '||SQLERRM);
      RAISE e_RECORD_FAIL;
  END Set_SDB_ATTRIBUTES;
    
  ----------------------------------------------------------------------------
  -- Constellar equivalent PRE/POST/etc procedures
  ----------------------------------------------------------------------------

  PROCEDURE PRE_INTERFACE IS
  BEGIN
    ------------------------------------------
    -- Constellar Hub : Pre Interface Rule
    ------------------------------------------
       HUB_LIB.GET_RUN_DATES(gc_INTERFACE_NAME, g_RUN_START, g_RUN_END);
       OFFSET_RUN_TS(g_RUN_START);
       
    -- Use an inline to populate the temp table QUT_SDB_HR_UPDATES with
    -- EMPLOYEE#s of any modified EMPLOYEE, SUBSTANTIVE, CONCURRENT, HDA
    BEGIN
      DELETE FROM QUT_SDB_HR_UPDATES;
      --
      INSERT INTO QUT_SDB_HR_UPDATES
        (SELECT DISTINCT(E.employee#)
           FROM employee    E
               ,substantive sd
          WHERE E.employee# = sd.employee#
            AND sd.classification NOT IN ('PRACT', 'SOC')
            AND ( E.tp BETWEEN g_RUN_START - 7
                           AND g_RUN_END       
               OR sd.tp BETWEEN g_RUN_START - 7
                            AND g_RUN_END
                )
        );

      COMMIT;
      
    END;

  END PRE_INTERFACE;


  PROCEDURE POST_INTERFACE IS
  BEGIN
    ------------------------------------------
    -- Constellar Hub : Post Interface Rule
    ------------------------------------------
    --  6 Jul 2011  DP  Add check for failed transaction
    IF NOT g_failed_trans THEN        
      HUB_LIB.SET_RUN_DATES(gc_INTERFACE_NAME, g_RUN_START, g_RUN_END);
    END IF;
    
  END POST_INTERFACE;


  ----------------------------------------------------------------------------
  -- Transactions
  ----------------------------------------------------------------------------

  PROCEDURE SDB_ORG_UNITS_TRANS IS
    c_trans_name CONSTANT VARCHAR2(60) := 'SDB_ORG_UNITS_TRANS';    

    -- CH: Collate 
    CURSOR codes_cr IS
      SELECT *
        FROM codes
       WHERE kind IN ('CLEVEL1','CLEVEL2','CLEVEL3','CLEVEL4','CLEVEL5');

    e_discard_record  EXCEPTION;

    l_key_data        VARCHAR2(100) := NULL;
    
    iteration         NUMBER   := NULL;
    l_src_rec_count   NUMBER   := 0;
    l_tgt_rec_count   NUMBER   := 0;
    l_discard_count   NUMBER   := 0;
    l_fail_count      NUMBER   := 0;
    
    l_tgt_rec         sdb_org_units%ROWTYPE := NULL;
  BEGIN
    HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, 
                       NULL, 'TRANSACTION started');
    
    SAVEPOINT Collate_SP;
    
    -- CH: Collate Cursor Loop
    <<PROCESS_CURSOR>>
    FOR codes_rec IN codes_cr
    LOOP
      l_key_data := 'CODE '||codes_rec.code;
                    
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'DEBUG', NULL, 
                         l_key_data, 'START processing RECORD.');

      iteration       := 0;  -- for reporting purposes
      l_src_rec_count := l_src_rec_count + 1;
      
      -- CH: [Replicate Loop]
        -- CH: Pre Record Actions 
        -- CH: Each Record Attributes 
          -- Prepare value for each field in the records and set them to a local variable
          l_tgt_rec := NULL;
          
          l_tgt_rec.description := SUBSTR(codes_rec.DESCRIPTION,1,40);
          
          l_tgt_rec.ORG_UNIT := codes_rec.CODE;
          
          l_tgt_rec.START_DATE := SYSDATE;
          
          l_tgt_rec_count := l_tgt_rec_count + 1;
          
          -- Perform insert, upsert or delete functions
          BEGIN
            Set_SDB_ORG_UNITS(l_tgt_rec, c_trans_name);
          EXCEPTION
            WHEN e_RECORD_FAIL  THEN
              l_fail_count := l_fail_count + 1;
              HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'WARN', NULL, 
                                 l_key_data||' - Iteration '||iteration, 
                                 'TARGET RECORD FAILURE');
          END;
        -- CH: Post Record Actions 
      -- CH: [Post Replicate]
    END LOOP PROCESS_CURSOR;
    
    COMMIT;    
    -- report completion status, record counts, etc
    HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, 
                       NULL, 'TRANSACTION completed'||
                       ' - SOURCE Records Processed: '||l_src_rec_count||
                       ' - Target Records Processed: '||l_tgt_rec_count||
                       ' - Discarded Records: '||l_discard_count||
                       ' - Failed Records: '||l_fail_count);
    
  EXCEPTION
    WHEN OTHERS THEN
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'ERROR', NULL,
                         l_key_data||' - Iteration '||iteration, 
                         'Transaction Failure - '||SQLERRM);
      ROLLBACK TO Collate_SP;
      RAISE e_TRANS_FAIL;
  END SDB_ORG_UNITS_TRANS;


  PROCEDURE POSITION_STRUCTURE  IS
    c_trans_name CONSTANT VARCHAR2(60) := 'POSITION_STRUCTURE';    
    ------------------------------------------
    -- Constellar Hub: Collate equivalent 
    ------------------------------------------
    CURSOR pos_structure_cr IS
       SELECT * 
         FROM position
        WHERE TP BETWEEN G_RUN_START - 7
                     AND G_RUN_END;

    e_discard_record  EXCEPTION;
    
    l_key_data        VARCHAR2(100) := NULL;
    
    iteration         NUMBER   := NULL;
    l_src_rec_count   NUMBER   := 0;
    l_tgt_rec_count   NUMBER   := 0;
    l_discard_count   NUMBER   := 0;
    l_fail_count      NUMBER   := 0;
    
    l_tgt_rec         sdb_attributes%ROWTYPE := NULL;
  BEGIN
    HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, 
                       NULL, 'TRANSACTION started');

    SAVEPOINT Collate_SP;

    ------------------------------------------
    -- Constellar Hub : For each Record in Collate Cursor
    ------------------------------------------
    <<PROCESS_CURSOR>>
    FOR ps_rec 
     IN pos_structure_cr
    LOOP
      l_key_data := 'POSITION# '||ps_rec.POSITION#||
                    ' | CLEVEL '||ps_rec.clevel;
                    
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'DEBUG', NULL, 
                         l_key_data, 'START processing RECORD.');
      ------------------------------------------
      -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
      ------------------------------------------
      iteration       := 0;  -- for reporting purposes
      l_src_rec_count := l_src_rec_count + 1;
       <<REPLICATE>>
      FOR iteration IN 1..3
      LOOP
        ------------------------------------------
        -- Constellar Hub : Pre Record Actions 
        ------------------------------------------
        DECLARE
          l_START_DATE DATE := NULL;
        BEGIN
          SELECT MAX(start_date)
            INTO l_START_DATE
            FROM POSITION
           WHERE POSITION# = ps_rec.POSITION#;
                
          IF ps_rec.START_DATE != l_START_DATE THEN
            RAISE e_discard_record  ;
          END IF;
        
          ------------------------------------------
          -- Constellar Hub : Each Record Actions
          ------------------------------------------        
          -- Prepare value for each field in the records and set them to a local variable
          l_tgt_rec := NULL;

          l_tgt_rec.attr_id := ps_rec.POSITION#;

          CASE iteration
          WHEN 1 THEN
            l_tgt_rec.attr_type := 'SUBSTANTIV';
          WHEN 2 THEN
            l_tgt_rec.attr_type := 'CONCURRENT';
          WHEN 3 THEN
            l_tgt_rec.attr_type := 'HDA';
          END CASE;

          l_tgt_rec.description := ps_rec.POS_TITLE;

          l_tgt_rec.end_date := ps_rec.end_date;

          l_tgt_rec.org_unit := ps_rec.clevel;

          l_tgt_rec.start_date := ps_rec.start_date;

          ------------------------------------------
          -- Constellar Hub : Actions = Insert, Update or Delete
          ------------------------------------------
          l_tgt_rec_count := l_tgt_rec_count + 1;
          
          -- Perform insert, upsert or delete functions
          Set_SDB_ATTRIBUTES(l_tgt_rec, c_trans_name);
          
          -- COMMIT by record goes here
          ------------------------------------------
          -- Constellar Hub : Post Record Actions 
          ------------------------------------------
        EXCEPTION
          WHEN e_RECORD_FAIL  THEN
            l_fail_count := l_fail_count + 1;
            HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'WARN', NULL, 
                               l_key_data||' - Iteration '||iteration, 
                               'TARGET RECORD FAILURE');
          WHEN e_discard_record THEN
            l_discard_count := l_discard_count + 1;
            HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'WARN', NULL, 
                               l_key_data||' - Iteration '||iteration, 
                               'Discarding RECORD.');
        END;
      END LOOP REPLICATE;    
        ------------------------------------------
        -- Constellar Hub : Post Replicate Actions 
        ------------------------------------------
    END LOOP PROCESS_CURSOR;    
    
    COMMIT;    
    -- report completion status, record counts, etc
    HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, 
                       NULL, 'TRANSACTION completed'||
                       ' - SOURCE Records Processed: '||l_src_rec_count||
                       ' - Target Records Processed: '||l_tgt_rec_count||
                       ' - Discarded Records: '||l_discard_count||
                       ' - Failed Records: '||l_fail_count);
    
  EXCEPTION
    WHEN OTHERS THEN
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'ERROR', NULL,
                         l_key_data||' - Iteration '||iteration, 
                         'Transaction Failure - '||SQLERRM);
      ROLLBACK TO Collate_SP;
      RAISE e_TRANS_FAIL;
  END POSITION_STRUCTURE;


  PROCEDURE STAFF_CLIENTS  IS
  -- 5 Jul 2011  DP  Modify cursor to look for changes to SUBSTANTIVE 
  --                 and CDS_CLIENT_ROLE (Redmine #2239)
    c_trans_name CONSTANT VARCHAR2(60) := 'STAFF_CLIENTS';    
    -- CH: Collate 
    CURSOR employee_cr IS
          SELECT emp.*
                ,ccr.trs_client_id AS CLIENT_ID
            FROM EMPLOYEE emp
                ,CDS_CLIENT_ROLE ccr
           WHERE emp.employee# = ccr.role_identity(+)
             AND ccr.role_cd(+) = 'STAFF'
             AND EXISTS
                (SELECT *
                   FROM substantive s
                  WHERE s.EMPLOYEE# = emp.employee#
                    AND hrm_qut_lib.EXT_SUBTERM(s.employee#, s.job#, s.commence_date) 
                                       > SYSDATE - 365)
             AND emp.employee# IN
                 (SELECT employee#
                    FROM employee
                   WHERE TP BETWEEN TRUNC(g_RUN_START) - 7
                                AND TRUNC(g_RUN_END)
                   UNION
                  SELECT DISTINCT employee#
                    FROM substantive
                   WHERE TP BETWEEN TRUNC(g_RUN_START) - 7
                                AND TRUNC(g_RUN_END)
                   UNION
                  SELECT role_identity AS "EMPLOYEE#"
                    FROM cds_client_role
                   WHERE role_cd = 'STAFF'
                     AND NVL(updated_dt, inserted_dt) 
                             BETWEEN TRUNC(g_RUN_START) - 7
                                 AND TRUNC(g_RUN_END)
                 );

    e_discard_record  EXCEPTION;

    l_key_data        VARCHAR2(100) := NULL;
    
    iteration         NUMBER   := NULL;
    l_src_rec_count   NUMBER   := 0;
    l_tgt_rec_count   NUMBER   := 0;
    l_discard_count   NUMBER   := 0;
    l_fail_count      NUMBER   := 0;
    
    l_tgt_rec         sdb_clients%ROWTYPE := NULL;
  BEGIN
    HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, 
                       NULL, 'TRANSACTION started');

    SAVEPOINT Collate_SP;

    -- CH: Collate Cursor Loop
    <<PROCESS_CURSOR>>
    FOR employee_rec IN employee_cr
    LOOP
      l_key_data := 'EMPLOYEE# '||employee_rec.employee#;
                    
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'DEBUG', NULL, 
                         l_key_data, 'START processing RECORD.');

      iteration       := 0;  -- for reporting purposes
      l_src_rec_count := l_src_rec_count + 1;
      
      -- CH: [Replicate Loop]
        -- CH: Pre Record Actions 
        DECLARE 
          l_client_id  l_tgt_rec.client_id%TYPE := NULL;
        BEGIN
          --l_client_id := Get_Client_ID(employee_rec.employee#);

          IF employee_rec.client_id IS NULL THEN
            HUB_LOG.LOG_WRITE 
                (gc_interface_name, c_trans_name, 'WARN', NULL, 
                 l_key_data||' - Iteration '||iteration,
                 'IAM has NOT created TRS client_id FOR EMP#:'||employee_rec.employee#|| 
                 ' - Discarding RECORD.');

              RAISE e_discard_record;
          END IF;

        -- CH: Each Record Attributes 
          -- Prepare value for each field in the records and set them to a local variable
          l_tgt_rec := NULL;
          
          l_tgt_rec.client_id := employee_rec.client_id;
                    
          l_tgt_rec.BIRTH_DATE := employee_rec.DATE_OF_BIRTH;
          
          l_tgt_rec.FIRST_NAME := employee_rec.FIRST_NAME;

          l_tgt_rec.FULL_NAME := NVL(employee_rec.PREFERRED_NAME, 
                                     employee_rec.FIRST_NAME
                                    )||' '|| employee_rec.SURNAME;

          l_tgt_rec.PREFERRED_FIRST_NAME := employee_rec.PREFERRED_NAME;

          l_tgt_rec.SECOND_NAME := employee_rec.SECOND_NAME;

          l_tgt_rec.SURNAME := employee_rec.SURNAME;

          l_tgt_rec.TITLE := employee_rec.TITLE;
                    
          l_tgt_rec_count := l_tgt_rec_count + 1;
          
          -- Perform insert, upsert or delete functions
          Set_SDB_CLIENTS(l_tgt_rec, c_trans_name);
          
        -- CH: Post Record Actions 
        -- CH: [Post Replicate]
        EXCEPTION
          WHEN e_RECORD_FAIL  THEN
            l_fail_count := l_fail_count + 1;
            g_failed_trans := TRUE;
            HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'WARN', NULL, 
                               l_key_data||' - Iteration '||iteration, 
                               'TARGET RECORD FAILURE');
          WHEN e_discard_record THEN
            l_discard_count := l_discard_count + 1;
            HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'WARN', NULL, 
                               l_key_data||' - Iteration '||iteration, 
                               'Discarding RECORD.');
        END;
    END LOOP PROCESS_CURSOR;
    
    COMMIT;    
    -- report completion status, record counts, etc
    HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, 
                       NULL, 'TRANSACTION completed'||
                       ' - SOURCE Records Processed: '||l_src_rec_count||
                       ' - Target Records Processed: '||l_tgt_rec_count||
                       ' - Discarded Records: '||l_discard_count||
                       ' - Failed Records: '||l_fail_count);
    
  EXCEPTION
    WHEN OTHERS THEN
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'ERROR', NULL,
                         l_key_data||' - Iteration '||iteration, 
                         'Transaction Failure - '||SQLERRM);
      ROLLBACK TO Collate_SP;
      RAISE e_TRANS_FAIL;
  END STAFF_CLIENTS;
  
  
  PROCEDURE STAFF_CLIENT_ROLES IS
    c_trans_name CONSTANT VARCHAR2(60) := 'STAFF_CLIENT_ROLES';    

    -- CH: Collate 
    CURSOR employee_cr IS
      SELECT emp.*
            ,ccr.trs_client_id AS CLIENT_ID
        FROM EMPLOYEE           emp
            ,QUT_SDB_HR_UPDATES qshu
            ,CDS_CLIENT_ROLE    ccr
       WHERE qshu.EMPLOYEE# = emp.EMPLOYEE#
         AND emp.FIRST_COMMENCE IS NOT NULL
         AND emp.employee# = ccr.role_identity(+)
         AND ccr.role_cd(+) = 'STAFF';      

    e_discard_record  EXCEPTION;

    l_key_data        VARCHAR2(100) := NULL;
    
    iteration         NUMBER   := NULL;
    l_src_rec_count   NUMBER   := 0;
    l_tgt_rec_count   NUMBER   := 0;
    l_discard_count   NUMBER   := 0;
    l_fail_count      NUMBER   := 0;
    
    l_tgt_rec         sdb_client_attributes%ROWTYPE := NULL;
  BEGIN
    HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, 
                       NULL, 'TRANSACTION started');

    SAVEPOINT Collate_SP;

    -- CH: Collate Cursor Loop
    <<PROCESS_CURSOR>>
    FOR employee_rec IN employee_cr
    LOOP
      l_key_data := 'EMPLOYEE# '||employee_rec.employee#;
                    
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'DEBUG', NULL, 
                         l_key_data, 'START processing RECORD.');

      iteration       := 0;  -- for reporting purposes
      l_src_rec_count := l_src_rec_count + 1;
      
      -- CH: [Replicate Loop]
        -- CH: Pre Record Actions 
        DECLARE 
          l_max_end_dt  DATE                     := NULL;
        BEGIN
          IF employee_rec.client_id IS NULL THEN
            HUB_LOG.LOG_WRITE 
                (gc_interface_name, c_trans_name, 'WARN', NULL, 
                 l_key_data||' - Iteration '||iteration,
                 'IAM has NOT created client_id FOR EMP#:'||employee_rec.employee#|| 
                 ' - Discarding RECORD.');

              RAISE e_discard_record;
          END IF;

          BEGIN
            SELECT MAX(hrm_qut_lib.EXT_SUBTERM(employee#, job#, commence_date))
            INTO   l_max_end_dt
            FROM   substantive 
            WHERE  EMPLOYEE# = employee_rec.employee#
            AND CLASSIFICATION NOT IN ('PRACT', 'SOC');
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              l_max_end_dt := NULL;
          END;
          
          IF l_max_end_dt IS NULL THEN
            HUB_LOG.LOG_WRITE 
                (gc_interface_name, c_trans_name, 'WARN', NULL, 
                 l_key_data||' - Iteration '||iteration,
                 'NO SUBSTANTIVEs FOUND FOR EMP#:'||employee_rec.employee#|| 
                 ' - Discarding RECORD.');

              RAISE e_discard_record;
          END IF;          

        -- CH: Each Record Attributes 
          -- Prepare value for each field in the records and set them to a local variable
          l_tgt_rec := NULL;
          
          l_tgt_rec.client_id  := employee_rec.client_id;
          
          l_tgt_rec.ATTR_DATA  := employee_rec.employee#;
          
          l_tgt_rec.ATTR_ID    := 'STAFF';
          
          l_tgt_rec.ATTR_TYPE  := 'ROLE';
          
          l_tgt_rec.END_DATE   := l_max_end_dt;
          
          l_tgt_rec.START_DATE := employee_rec.FIRST_COMMENCE;
          
          l_tgt_rec_count := l_tgt_rec_count + 1;
          
          -- Perform insert, upsert or delete functions
          Set_SDB_CLIENT_ATTRIBUTES(l_tgt_rec, c_trans_name);
          
        -- CH: Post Record Actions 
        -- CH: [Post Replicate]
        EXCEPTION
          WHEN e_RECORD_FAIL  THEN
            l_fail_count := l_fail_count + 1;
            g_failed_trans := TRUE;
            HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'WARN', NULL, 
                               l_key_data||' - Iteration '||iteration, 
                               'TARGET RECORD FAILURE');
          WHEN e_discard_record THEN
            l_discard_count := l_discard_count + 1;
            HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'WARN', NULL, 
                               l_key_data||' - Iteration '||iteration, 
                               'Discarding RECORD.');
        END;
    END LOOP PROCESS_CURSOR;
    
    COMMIT;    
    -- report completion status, record counts, etc
    HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, 
                       NULL, 'TRANSACTION completed'||
                       ' - SOURCE Records Processed: '||l_src_rec_count||
                       ' - Target Records Processed: '||l_tgt_rec_count||
                       ' - Discarded Records: '||l_discard_count||
                       ' - Failed Records: '||l_fail_count);
    
  EXCEPTION
    WHEN OTHERS THEN
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'ERROR', NULL,
                         l_key_data||' - Iteration '||iteration, 
                         'Transaction Failure - '||SQLERRM);
      ROLLBACK TO Collate_SP;
      RAISE e_TRANS_FAIL;
  END STAFF_CLIENT_ROLES;


  PROCEDURE STAFF_IDCARD_ROLES IS
    c_trans_name CONSTANT VARCHAR2(60) := 'STAFF_IDCARD_ROLES';

    -- CH: Collate
    CURSOR idcard_cr IS
      SELECT 
             ccr.trs_client_id AS client_id
            ,ic.role_id AS client_number
            ,ic.issue_level AS card_issue_level
            ,ic.issue_dt
            ,ic.expiry_dt
            ,ic.cardax_number
            ,ic.cardax_issue_level
            ,ic.front_barcode
            ,ic.card_barcode
        FROM 
             ID_CARD ic
            ,CDS_CLIENT_ROLE ccr
       WHERE 
             ic.role_cd = 'STAFF'
         AND ic.issue_level = (SELECT MAX(ic2.issue_level)
                                FROM ID_CARD ic2
                                WHERE
                                      ic2.role_cd = ic.role_cd
                                  AND ic2.role_id = ic.role_id)
         AND ic.role_id = ccr.role_identity(+)
         AND ccr.role_cd(+) = 'STAFF'
         AND (ic.updated_dt BETWEEN g_RUN_START - 1 AND g_RUN_END 
          OR ic.issue_dt BETWEEN g_RUN_START - 1 AND g_RUN_END);

    e_discard_record  EXCEPTION;

    l_key_data        VARCHAR2(100) := NULL;

    iteration         NUMBER   := NULL;
    l_src_rec_count   NUMBER   := 0;
    l_tgt_rec_count   NUMBER   := 0;
    l_discard_count   NUMBER   := 0;
    l_fail_count      NUMBER   := 0;

    l_tmp_rec         sdb_client_attributes%ROWTYPE := NULL;

    l_tgt_rec         sdb_client_attributes%ROWTYPE := NULL;
  BEGIN
    HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL,
                       NULL, 'TRANSACTION started');

    SAVEPOINT Collate_SP;

    -- CH: Collate Cursor Loop
    <<PROCESS_CURSOR>>
    FOR idcard_rec IN idcard_cr
    LOOP
      l_key_data := 'EMP ' || idcard_rec.client_number;

      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'DEBUG', NULL,
                         l_key_data, 'START processing RECORD.');

      iteration       := 0;  -- for reporting purposes
      l_src_rec_count := l_src_rec_count + 1;
      BEGIN
        IF idcard_rec.client_id IS NULL THEN
          HUB_LOG.LOG_WRITE
              (gc_interface_name, c_trans_name, 'WARN', NULL,
               l_key_data || ' - Iteration ' || iteration,
               'IAM has NOT created client_id FOR EMP#:' || idcard_rec.client_number ||
               ' - Discarding RECORD.');

            RAISE e_discard_record;
        END IF;

        BEGIN
          l_tmp_rec := NULL;
          SELECT *
            INTO l_tmp_rec
            FROM sdb_client_attributes sca
           WHERE sca.client_id = idcard_rec.client_id
             AND sca.attr_type = 'ROLE'
             AND sca.attr_id   = 'STAFF';
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            l_tmp_rec := NULL;
        END;

        IF l_tmp_rec.client_id IS NULL THEN
          HUB_LOG.LOG_WRITE
              (gc_interface_name, c_trans_name, 'WARN', NULL,
               l_key_data || ' - Iteration ' || iteration,
               'Staff ROLE NOT FOUND IN SDB FOR client: ' || idcard_rec.client_id ||
               ' - Employee#: ' || idcard_rec.client_number ||
               ' - Discarding RECORD.');

            RAISE e_discard_record;
        END IF;

        -- CH: [Replicate Loop]
         <<REPLICATE>>
        FOR iteration IN 1..2
        LOOP
          -- CH: Pre Record Actions
          IF iteration = 2 AND (idcard_rec.expiry_dt <= SYSDATE OR idcard_rec.issue_dt > SYSDATE) THEN
            RAISE e_discard_record;
          END IF;

		-- CH: Each Record Attributes
          -- Prepare value for each field in the records and set them to a local variable
          l_tgt_rec := NULL;
          -- Assign target attributes

          l_tgt_rec.client_id  := idcard_rec.client_id;

          IF iteration = 1 THEN -- IDCard attribute
            l_tgt_rec.ATTR_DATA := REPLACE(idcard_rec.front_barcode, ' ');
          ELSE -- CARDAX attribute
            l_tgt_rec.ATTR_DATA := idcard_rec.card_barcode;
          END IF;

          IF iteration = 1 THEN -- IDCard attribute
            l_tgt_rec.ATTR_ID := 'STAFF';
          ELSE -- CARDAX attribute
            l_tgt_rec.ATTR_ID := 'CARDAX';
          END IF;

          l_tgt_rec.ATTR_TYPE := 'IDCARD';

          l_tgt_rec.END_DATE  := idcard_rec.EXPIRY_DT;

          l_tgt_rec.OWN_ATTR_ID := 'STAFF';

          IF iteration = 1 THEN -- IDCard attribute
            l_tgt_rec.OWN_ATTR_TYPE := 'ROLE';
          ELSE -- CARDAX attribute
            l_tgt_rec.OWN_ATTR_TYPE := 'IDCARD';
          END IF;

          l_tgt_rec.own_client_id  := idcard_rec.client_id;

          IF iteration = 1 THEN -- IDCard attribute
            l_tgt_rec.START_DATE := NULL;
          ELSE  -- CARDAX attribute
            l_tgt_rec.START_DATE := idcard_rec.issue_dt;  
          END IF;

          l_tgt_rec_count := l_tgt_rec_count + 1;

          -- Perform insert, upsert or delete functions
          Set_SDB_CLIENT_ATTRIBUTES(l_tgt_rec, c_trans_name);

          -- CH: Post Record Actions
        END LOOP REPLICATE;
        -- CH: [Post Replicate]
      EXCEPTION
        WHEN e_RECORD_FAIL  THEN
          l_fail_count := l_fail_count + 1;
          g_failed_trans := TRUE;
          HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'WARN', NULL,
                             l_key_data || ' - Iteration ' || iteration,
                             'TARGET RECORD FAILURE');
        WHEN e_discard_record THEN
          l_discard_count := l_discard_count + 1;
          HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'WARN', NULL,
                             l_key_data || ' - Iteration ' || iteration,
                             'Discarding RECORD.');
      END;
    END LOOP PROCESS_CURSOR;

    COMMIT;
    -- report completion status, record counts, etc
    HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL,
                       NULL, 'TRANSACTION completed'||
                       ' - SOURCE Records Processed: ' || l_src_rec_count ||
                       ' - Target Records Processed: ' || l_tgt_rec_count ||
                       ' - Discarded Records: ' || l_discard_count ||
                       ' - Failed Records: ' || l_fail_count);

  EXCEPTION
    WHEN OTHERS THEN
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'ERROR', NULL,
                         l_key_data || ' - Iteration ' || iteration,
                         'Transaction Failure - ' || SQLERRM);
      ROLLBACK TO Collate_SP;
      RAISE e_TRANS_FAIL;
  END STAFF_IDCARD_ROLES;


  PROCEDURE STAFF_SUBSTANTIVE_DELETE IS
    c_trans_name CONSTANT VARCHAR2(60) := 'STAFF_SUBSTANTIVE_DELETE';    

    -- CH: Collate 
    CURSOR sdel_cr IS
      SELECT sdl.*
            ,ccr.trs_client_id AS CLIENT_ID
        FROM substantive_del  sdl
            ,CDS_CLIENT_ROLE ccr
       WHERE sdl.TP_TYPE = 'D'
         AND sdl.TP BETWEEN g_RUN_START - 2
                        AND g_RUN_END
         AND sdl.EMPLOYEE#   = ccr.role_identity(+)
         AND ccr.role_cd(+)  = 'STAFF';

    e_discard_record  EXCEPTION;

    l_key_data        VARCHAR2(100) := NULL;
    
    iteration         NUMBER   := NULL;
    l_src_rec_count   NUMBER   := 0;
    l_tgt_rec_count   NUMBER   := 0;
    l_discard_count   NUMBER   := 0;
    l_fail_count      NUMBER   := 0;
    
    l_tgt_rec         sdb_client_attributes%ROWTYPE := NULL;
  BEGIN
    HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, 
                       NULL, 'TRANSACTION started');

    SAVEPOINT Collate_SP;

    -- CH: Collate Cursor Loop
    <<PROCESS_CURSOR>>
    FOR sdel_rec IN sdel_cr
    LOOP
      l_key_data := 'EMPLOYEE '||sdel_rec.employee#;
                    
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'DEBUG', NULL, 
                         l_key_data, 'START processing RECORD.');

      iteration       := 0;  -- for reporting purposes
      l_src_rec_count := l_src_rec_count + 1;
       
      BEGIN
        IF sdel_rec.client_id IS NULL THEN
          HUB_LOG.LOG_WRITE 
              (gc_interface_name, c_trans_name, 'WARN', NULL, 
               l_key_data||' - Iteration '||iteration,
               'IAM has NOT created client_id FOR EMP#:'||sdel_rec.employee#|| 
               ' - Discarding RECORD.');

            RAISE e_discard_record;
        END IF;
      
      -- CH: [Replicate Loop]
        -- CH: Pre Record Actions 
        -- CH: Each Record Attributes 
          -- Prepare value for each field in the records and set them to a local variable
          l_tgt_rec := NULL;
          
          -- Assign a target attribute
          l_tgt_rec.client_id  := sdel_rec.client_id;
            
          l_tgt_rec.ATTR_ID    := sdel_rec.POSITION#;

          l_tgt_rec.ATTR_TYPE  := 'SUBSTANTIV';
                    
          l_tgt_rec_count := l_tgt_rec_count + 1;
          
          -- Perform insert, upsert or delete functions
          Del_SDB_CLIENT_ATTRIBUTES(l_tgt_rec, c_trans_name);
          
        -- CH: Post Record Actions 
      -- CH: [Post Replicate]
      EXCEPTION
        WHEN e_RECORD_FAIL  THEN
          l_fail_count := l_fail_count + 1;
          g_failed_trans := TRUE;
          HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'WARN', NULL, 
                             l_key_data||' - Iteration '||iteration, 
                             'TARGET RECORD FAILURE');
        WHEN e_discard_record THEN
          l_discard_count := l_discard_count + 1;
          HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'WARN', NULL, 
                             l_key_data||' - Iteration '||iteration, 
                             'Discarding RECORD.');
      END;
    END LOOP PROCESS_CURSOR;
    
    COMMIT;    
    -- report completion status, record counts, etc
    HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, 
                       NULL, 'TRANSACTION completed'||
                       ' - SOURCE Records Processed: '||l_src_rec_count||
                       ' - Target Records Processed: '||l_tgt_rec_count||
                       ' - Discarded Records: '||l_discard_count||
                       ' - Failed Records: '||l_fail_count);
    
  EXCEPTION
    WHEN OTHERS THEN
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'ERROR', NULL,
                         l_key_data||' - Iteration '||iteration, 
                         'Transaction Failure - '||SQLERRM);
      ROLLBACK TO Collate_SP;
      RAISE e_TRANS_FAIL;
  END STAFF_SUBSTANTIVE_DELETE;
  

  PROCEDURE STAFF_HDA_DELETE IS
    c_trans_name CONSTANT VARCHAR2(60) := 'STAFF_HDA_DELETE';    

    -- CH: Collate 
    CURSOR hdel_cr IS
      SELECT hdl.*
            ,ccr.trs_client_id AS CLIENT_ID
        FROM hda_del         hdl
            ,CDS_CLIENT_ROLE ccr
       WHERE hdl.TP_TYPE = 'D'
         AND hdl.TP BETWEEN g_RUN_START - 2
                        AND g_RUN_END
         AND hdl.EMPLOYEE#   = ccr.role_identity(+)
         AND ccr.role_cd(+)  = 'STAFF';

    e_discard_record  EXCEPTION;

    l_key_data        VARCHAR2(100) := NULL;
    
    iteration         NUMBER   := NULL;
    l_src_rec_count   NUMBER   := 0;
    l_tgt_rec_count   NUMBER   := 0;
    l_discard_count   NUMBER   := 0;
    l_fail_count      NUMBER   := 0;
    
    l_tgt_rec         sdb_client_attributes%ROWTYPE := NULL;
  BEGIN
    HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, 
                       NULL, 'TRANSACTION started');

    SAVEPOINT Collate_SP;

    -- CH: Collate Cursor Loop
    <<PROCESS_CURSOR>>
    FOR hdel_rec IN hdel_cr
    LOOP
      l_key_data := 'EMPLOYEE '||hdel_rec.employee#;
                    
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'DEBUG', NULL, 
                         l_key_data, 'START processing RECORD.');

      iteration       := 0;  -- for reporting purposes
      l_src_rec_count := l_src_rec_count + 1;
       
      BEGIN
        IF hdel_rec.client_id IS NULL THEN
          HUB_LOG.LOG_WRITE 
              (gc_interface_name, c_trans_name, 'WARN', NULL, 
               l_key_data||' - Iteration '||iteration,
               'IAM has NOT created client_id FOR EMP#:'||hdel_rec.employee#|| 
               ' - Discarding RECORD.');

            RAISE e_discard_record;
        END IF;
      
      -- CH: [Replicate Loop]
        -- CH: Pre Record Actions 
        -- CH: Each Record Attributes 
          -- Prepare value for each field in the records and set them to a local variable
          l_tgt_rec := NULL;
          
          -- Assign a target attribute
          l_tgt_rec.client_id  := hdel_rec.client_id;
            
          l_tgt_rec.ATTR_ID    := hdel_rec.POSITION#;

          l_tgt_rec.ATTR_TYPE  := 'HDA';
                    
          l_tgt_rec_count := l_tgt_rec_count + 1;
          
          -- Perform insert, upsert or delete functions
          Del_SDB_CLIENT_ATTRIBUTES(l_tgt_rec, c_trans_name);
          
        -- CH: Post Record Actions 
      -- CH: [Post Replicate]
      EXCEPTION
        WHEN e_RECORD_FAIL  THEN
          l_fail_count := l_fail_count + 1;
          g_failed_trans := TRUE;
          HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'WARN', NULL, 
                             l_key_data||' - Iteration '||iteration, 
                             'TARGET RECORD FAILURE');
        WHEN e_discard_record THEN
          l_discard_count := l_discard_count + 1;
          HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'WARN', NULL, 
                             l_key_data||' - Iteration '||iteration, 
                             'Discarding RECORD.');
      END;
    END LOOP PROCESS_CURSOR;
    
    COMMIT;    
    -- report completion status, record counts, etc
    HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, 
                       NULL, 'TRANSACTION completed'||
                       ' - SOURCE Records Processed: '||l_src_rec_count||
                       ' - Target Records Processed: '||l_tgt_rec_count||
                       ' - Discarded Records: '||l_discard_count||
                       ' - Failed Records: '||l_fail_count);
    
  EXCEPTION
    WHEN OTHERS THEN
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'ERROR', NULL,
                         l_key_data||' - Iteration '||iteration, 
                         'Transaction Failure - '||SQLERRM);
      ROLLBACK TO Collate_SP;
      RAISE e_TRANS_FAIL;
  END STAFF_HDA_DELETE;
  

  PROCEDURE STAFF_CONCURRENT_DELETE IS
    c_trans_name CONSTANT VARCHAR2(60) := 'STAFF_CONCURRENT_DELETE';    

    -- CH: Collate 
    CURSOR cdel_cr IS
      SELECT cdl.*
            ,ccr.trs_client_id AS CLIENT_ID
        FROM concurrent_del  cdl
            ,CDS_CLIENT_ROLE ccr
       WHERE cdl.TP_TYPE = 'D'
         AND cdl.TP BETWEEN g_RUN_START - 2
                        AND g_RUN_END
         AND cdl.EMPLOYEE#   = ccr.role_identity(+)
         AND ccr.role_cd(+)  = 'STAFF';

    e_discard_record  EXCEPTION;

    l_key_data        VARCHAR2(100) := NULL;
    
    iteration         NUMBER   := NULL;
    l_src_rec_count   NUMBER   := 0;
    l_tgt_rec_count   NUMBER   := 0;
    l_discard_count   NUMBER   := 0;
    l_fail_count      NUMBER   := 0;
    
    l_tgt_rec         sdb_client_attributes%ROWTYPE := NULL;
  BEGIN
    HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, 
                       NULL, 'TRANSACTION started');

    SAVEPOINT Collate_SP;

    -- CH: Collate Cursor Loop
    <<PROCESS_CURSOR>>
    FOR cdel_rec IN cdel_cr
    LOOP
      l_key_data := 'EMPLOYEE '||cdel_rec.employee#;
                    
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'DEBUG', NULL, 
                         l_key_data, 'START processing RECORD.');

      iteration       := 0;  -- for reporting purposes
      l_src_rec_count := l_src_rec_count + 1;
       
      BEGIN
        IF cdel_rec.client_id IS NULL THEN
          HUB_LOG.LOG_WRITE 
              (gc_interface_name, c_trans_name, 'WARN', NULL, 
               l_key_data||' - Iteration '||iteration,
               'IAM has NOT created client_id FOR EMP#:'||cdel_rec.employee#|| 
               ' - Discarding RECORD.');

            RAISE e_discard_record;
        END IF;
      
      -- CH: [Replicate Loop]
        -- CH: Pre Record Actions 
        -- CH: Each Record Attributes 
          -- Prepare value for each field in the records and set them to a local variable
          l_tgt_rec := NULL;
          
          -- Assign a target attribute
          l_tgt_rec.client_id  := cdel_rec.client_id;
            
          l_tgt_rec.ATTR_ID    := cdel_rec.POSITION#;

          l_tgt_rec.ATTR_TYPE  := 'CONCURRENT';
                    
          l_tgt_rec_count := l_tgt_rec_count + 1;
          
          -- Perform insert, upsert or delete functions
          Del_SDB_CLIENT_ATTRIBUTES(l_tgt_rec, c_trans_name);
          
        -- CH: Post Record Actions 
      -- CH: [Post Replicate]
      EXCEPTION
        WHEN e_RECORD_FAIL  THEN
          l_fail_count := l_fail_count + 1;
          g_failed_trans := TRUE;
          HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'WARN', NULL, 
                             l_key_data||' - Iteration '||iteration, 
                             'TARGET RECORD FAILURE');
        WHEN e_discard_record THEN
          l_discard_count := l_discard_count + 1;
          HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'WARN', NULL, 
                             l_key_data||' - Iteration '||iteration, 
                             'Discarding RECORD.');
      END;
    END LOOP PROCESS_CURSOR;
    
    COMMIT;    
    -- report completion status, record counts, etc
    HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, 
                       NULL, 'TRANSACTION completed'||
                       ' - SOURCE Records Processed: '||l_src_rec_count||
                       ' - Target Records Processed: '||l_tgt_rec_count||
                       ' - Discarded Records: '||l_discard_count||
                       ' - Failed Records: '||l_fail_count);
    
  EXCEPTION
    WHEN OTHERS THEN
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'ERROR', NULL,
                         l_key_data||' - Iteration '||iteration, 
                         'Transaction Failure - '||SQLERRM);
      ROLLBACK TO Collate_SP;
      RAISE e_TRANS_FAIL;
  END STAFF_CONCURRENT_DELETE;
  
  
  PROCEDURE STAFF_SUBSTANTIVE_ROLES IS
    c_trans_name CONSTANT VARCHAR2(60) := 'STAFF_SUBSTANTIVE_ROLES';    

    -- CH: Collate 
    CURSOR sub_cr IS
      SELECT sub.employee#
            ,sub.commence_date
            ,hrm_qut_lib.EXT_SUBTERM(sub.employee#, sub.job#, sub.commence_date) 
                    AS OCCUP_TERM_DATE
            ,sub.position#
            ,sub.pos_fraction
            ,sub.occup_com_reas
            ,ccr.trs_client_id AS CLIENT_ID
        FROM EMPLOYEE         emp
            ,SUBSTANTIVE      sub
            ,CDS_CLIENT_ROLE  ccr
       WHERE sub.EMPLOYEE#       = emp.EMPLOYEE#
         AND hrm_qut_lib.EXT_SUBTERM(sub.employee#, sub.job#, sub.commence_date) >= SYSDATE
         AND emp.FIRST_COMMENCE  IS NOT NULL
         AND emp.employee#      = ccr.role_identity(+)
         AND ccr.role_cd(+)     = 'STAFF'
         AND ( (emp.TP BETWEEN g_RUN_START - 7 AND g_RUN_END)
            OR (sub.TP  BETWEEN g_RUN_START - 7 AND g_RUN_END)
             )
         AND sub.commence_date = 
             (SELECT MAX(commence_date)
                FROM substantive
               WHERE EMPLOYEE# = sub.EMPLOYEE#
                 AND JOB#      = sub.JOB#
                 AND classification NOT IN ('PRACT', 'SOC'));
             
    e_discard_record  EXCEPTION;

    l_key_data        VARCHAR2(100) := NULL;
    
    iteration         NUMBER   := NULL;
    l_src_rec_count   NUMBER   := 0;
    l_tgt_rec_count   NUMBER   := 0;
    l_discard_count   NUMBER   := 0;
    l_fail_count      NUMBER   := 0;
    
    l_tgt_rec         sdb_client_attributes%ROWTYPE := NULL;
  BEGIN
    HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, 
                       NULL, 'TRANSACTION started');

    SAVEPOINT Collate_SP;

    -- CH: Collate Cursor Loop
    <<PROCESS_CURSOR>>
    FOR sub_rec IN sub_cr
    LOOP
      l_key_data := 'EMPLOYEE# '||sub_rec.employee#;
                    
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'DEBUG', NULL, 
                         l_key_data, 'START processing RECORD.');

      iteration       := 0;  -- for reporting purposes
      l_src_rec_count := l_src_rec_count + 1;
      
      -- CH: [Replicate Loop]
        -- CH: Pre Record Actions 
        DECLARE 
          l_max_start_dt  DATE                     := NULL;
        BEGIN
          IF sub_rec.client_id IS NULL THEN
            HUB_LOG.LOG_WRITE 
                (gc_interface_name, c_trans_name, 'WARN', NULL, 
                 l_key_data||' - Iteration '||iteration,
                 'IAM has NOT created client_id FOR EMP#:'||sub_rec.employee#|| 
                 ' - Discarding RECORD.');

              RAISE e_discard_record;
          END IF;

        -- CH: Each Record Attributes 
          -- Prepare value for each field in the records and set them to a local variable
          l_tgt_rec := NULL;
          
          l_tgt_rec.client_id     := sub_rec.client_id;
          
          l_tgt_rec.ATTR_DATA     := sub_rec.POS_FRACTION||' | '||
                                     sub_rec.OCCUP_COM_REAS;
          
          l_tgt_rec.ATTR_ID       := sub_rec.POSITION#;
          
          l_tgt_rec.ATTR_TYPE     := 'SUBSTANTIV';
          
          l_tgt_rec.END_DATE      := sub_rec.OCCUP_TERM_DATE;
          
          l_tgt_rec.START_DATE    := sub_rec.COMMENCE_DATE;
          
          l_tgt_rec.OWN_ATTR_ID   := 'STAFF';

          l_tgt_rec.OWN_ATTR_TYPE := 'ROLE';

          l_tgt_rec.own_client_id := sub_rec.client_id;

          l_tgt_rec_count := l_tgt_rec_count + 1;
          
          -- Perform insert, upsert or delete functions
          Set_SDB_CLIENT_ATTRIBUTES(l_tgt_rec, c_trans_name);
          
        -- CH: Post Record Actions 
        -- CH: [Post Replicate]
        EXCEPTION
          WHEN e_RECORD_FAIL  THEN
            l_fail_count := l_fail_count + 1;
            g_failed_trans := TRUE;
            HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'WARN', NULL, 
                               l_key_data||' - Iteration '||iteration, 
                               'TARGET RECORD FAILURE');
          WHEN e_discard_record THEN
            l_discard_count := l_discard_count + 1;
            HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'WARN', NULL, 
                               l_key_data||' - Iteration '||iteration, 
                               'Discarding RECORD.');
        END;
    END LOOP PROCESS_CURSOR;
    
    COMMIT;    
    -- report completion status, record counts, etc
    HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, 
                       NULL, 'TRANSACTION completed'||
                       ' - SOURCE Records Processed: '||l_src_rec_count||
                       ' - Target Records Processed: '||l_tgt_rec_count||
                       ' - Discarded Records: '||l_discard_count||
                       ' - Failed Records: '||l_fail_count);
    
  EXCEPTION
    WHEN OTHERS THEN
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'ERROR', NULL,
                         l_key_data||' - Iteration '||iteration, 
                         'Transaction Failure - '||SQLERRM);
      ROLLBACK TO Collate_SP;
      RAISE e_TRANS_FAIL;
  END STAFF_SUBSTANTIVE_ROLES;
  
  
  PROCEDURE STAFF_HDA_ROLES IS
    c_trans_name CONSTANT VARCHAR2(60) := 'STAFF_HDA_ROLES';    

    -- CH: Collate 
    CURSOR hda_cr IS
      SELECT hda.*
            ,ccr.trs_client_id AS CLIENT_ID
        FROM EMPLOYEE         emp
            ,HDA
            ,CDS_CLIENT_ROLE  ccr
       WHERE hda.EMPLOYEE#       = emp.EMPLOYEE#
         AND hda.OCCUP_TERM_DATE >= SYSDATE
         AND emp.FIRST_COMMENCE  IS NOT NULL
         AND emp.employee#      = ccr.role_identity(+)
         AND ccr.role_cd(+)     = 'STAFF'
         AND ( (emp.TP BETWEEN g_RUN_START AND g_RUN_END)
            OR (hda.TP  BETWEEN g_RUN_START AND g_RUN_END)
             )
         AND hda.commence_date = 
             (SELECT MAX(commence_date)
                FROM hda
               WHERE EMPLOYEE# = hda.EMPLOYEE#
                 AND JOB#      = hda.JOB#);
             
    e_discard_record  EXCEPTION;

    l_key_data        VARCHAR2(100) := NULL;
    
    iteration         NUMBER   := NULL;
    l_src_rec_count   NUMBER   := 0;
    l_tgt_rec_count   NUMBER   := 0;
    l_discard_count   NUMBER   := 0;
    l_fail_count      NUMBER   := 0;
    
    l_tgt_rec         sdb_client_attributes%ROWTYPE := NULL;
  BEGIN
    HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, 
                       NULL, 'TRANSACTION started');

    SAVEPOINT Collate_SP;

    -- CH: Collate Cursor Loop
    <<PROCESS_CURSOR>>
    FOR hda_rec IN hda_cr
    LOOP
      l_key_data := 'EMPLOYEE# '||hda_rec.employee#;
                    
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'DEBUG', NULL, 
                         l_key_data, 'START processing RECORD.');

      iteration       := 0;  -- for reporting purposes
      l_src_rec_count := l_src_rec_count + 1;
      
      -- CH: [Replicate Loop]
        -- CH: Pre Record Actions 
        DECLARE 
          l_max_start_dt  DATE                     := NULL;
        BEGIN
          IF hda_rec.client_id IS NULL THEN
            HUB_LOG.LOG_WRITE 
                (gc_interface_name, c_trans_name, 'WARN', NULL, 
                 l_key_data||' - Iteration '||iteration,
                 'IAM has NOT created client_id FOR EMP#:'||hda_rec.employee#|| 
                 ' - Discarding RECORD.');

              RAISE e_discard_record;
          END IF;

        -- CH: Each Record Attributes 
          -- Prepare value for each field in the records and set them to a local variable
          l_tgt_rec := NULL;
          
          l_tgt_rec.client_id     := hda_rec.client_id;
          
          l_tgt_rec.ATTR_DATA     := hda_rec.POS_FRACTION||' | '||
                                     hda_rec.OCCUP_COM_REAS;
          
          l_tgt_rec.ATTR_ID       := hda_rec.POSITION#;
          
          l_tgt_rec.ATTR_TYPE     := 'HDA';
          
          l_tgt_rec.END_DATE      := hda_rec.OCCUP_TERM_DATE;
          
          l_tgt_rec.START_DATE    := hda_rec.COMMENCE_DATE;
          
          l_tgt_rec.OWN_ATTR_ID   := 'STAFF';

          l_tgt_rec.OWN_ATTR_TYPE := 'ROLE';

          l_tgt_rec.own_client_id := hda_rec.client_id;

          l_tgt_rec_count := l_tgt_rec_count + 1;
          
          -- Perform insert, upsert or delete functions
          Set_SDB_CLIENT_ATTRIBUTES(l_tgt_rec, c_trans_name);
          
        -- CH: Post Record Actions 
        -- CH: [Post Replicate]
        EXCEPTION
          WHEN e_RECORD_FAIL  THEN
            l_fail_count := l_fail_count + 1;
            g_failed_trans := TRUE;
            HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'WARN', NULL, 
                               l_key_data||' - Iteration '||iteration, 
                               'TARGET RECORD FAILURE');
          WHEN e_discard_record THEN
            l_discard_count := l_discard_count + 1;
            HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'WARN', NULL, 
                               l_key_data||' - Iteration '||iteration, 
                               'Discarding RECORD.');
        END;
    END LOOP PROCESS_CURSOR;
    
    COMMIT;    
    -- report completion status, record counts, etc
    HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, 
                       NULL, 'TRANSACTION completed'||
                       ' - SOURCE Records Processed: '||l_src_rec_count||
                       ' - Target Records Processed: '||l_tgt_rec_count||
                       ' - Discarded Records: '||l_discard_count||
                       ' - Failed Records: '||l_fail_count);
    
  EXCEPTION
    WHEN OTHERS THEN
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'ERROR', NULL,
                         l_key_data||' - Iteration '||iteration, 
                         'Transaction Failure - '||SQLERRM);
      ROLLBACK TO Collate_SP;
      RAISE e_TRANS_FAIL;
  END STAFF_HDA_ROLES;
  
  
  PROCEDURE STAFF_CONCURRENT_ROLES IS
    c_trans_name CONSTANT VARCHAR2(60) := 'STAFF_CONCURRENT_ROLES';    

    -- CH: Collate 
    CURSOR con_cr IS
      SELECT con.*
            ,ccr.trs_client_id AS CLIENT_ID
        FROM EMPLOYEE         emp
            ,CONCURRENT       con
            ,CDS_CLIENT_ROLE  ccr
       WHERE con.EMPLOYEE#       = emp.EMPLOYEE#
         AND con.OCCUP_TERM_DATE >= SYSDATE
         AND emp.FIRST_COMMENCE  IS NOT NULL
         AND emp.employee#      = ccr.role_identity(+)
         AND ccr.role_cd(+)     = 'STAFF'
         AND ( (emp.TP BETWEEN g_RUN_START AND g_RUN_END)
            OR (con.TP  BETWEEN g_RUN_START AND g_RUN_END)
             )
         AND con.commence_date = 
             (SELECT MAX(commence_date)
                FROM substantive
               WHERE EMPLOYEE# = con.EMPLOYEE#
                 AND JOB#      = con.JOB#
                 AND classification NOT IN ('PRACT', 'SOC'));
             
    e_discard_record  EXCEPTION;

    l_key_data        VARCHAR2(100) := NULL;
    
    iteration         NUMBER   := NULL;
    l_src_rec_count   NUMBER   := 0;
    l_tgt_rec_count   NUMBER   := 0;
    l_discard_count   NUMBER   := 0;
    l_fail_count      NUMBER   := 0;
    
    l_tgt_rec         sdb_client_attributes%ROWTYPE := NULL;
  BEGIN
    HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, 
                       NULL, 'TRANSACTION started');

    SAVEPOINT Collate_SP;

    -- CH: Collate Cursor Loop
    <<PROCESS_CURSOR>>
    FOR con_rec IN con_cr
    LOOP
      l_key_data := 'EMPLOYEE# '||con_rec.employee#;
                    
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'DEBUG', NULL, 
                         l_key_data, 'START processing RECORD.');

      iteration       := 0;  -- for reporting purposes
      l_src_rec_count := l_src_rec_count + 1;
      
      -- CH: [Replicate Loop]
        -- CH: Pre Record Actions 
        DECLARE 
          l_max_start_dt  DATE                     := NULL;
        BEGIN
          IF con_rec.client_id IS NULL THEN
            HUB_LOG.LOG_WRITE 
                (gc_interface_name, c_trans_name, 'WARN', NULL, 
                 l_key_data||' - Iteration '||iteration,
                 'IAM has NOT created client_id FOR EMP#:'||con_rec.employee#|| 
                 ' - Discarding RECORD.');

              RAISE e_discard_record;
          END IF;
          
        -- CH: Each Record Attributes 
          -- Prepare value for each field in the records and set them to a local variable
          l_tgt_rec := NULL;
          
          l_tgt_rec.client_id     := con_rec.client_id;
          
          l_tgt_rec.ATTR_DATA     := con_rec.POS_FRACTION||' | '||
                                     con_rec.OCCUP_COM_REAS;
          
          l_tgt_rec.ATTR_ID       := con_rec.POSITION#;
          
          l_tgt_rec.ATTR_TYPE     := 'CONCURRENT';
          
          l_tgt_rec.END_DATE      := con_rec.OCCUP_TERM_DATE;
          
          l_tgt_rec.START_DATE    := con_rec.COMMENCE_DATE;
          
          l_tgt_rec.OWN_ATTR_ID   := 'STAFF';

          l_tgt_rec.OWN_ATTR_TYPE := 'ROLE';

          l_tgt_rec.own_client_id := con_rec.client_id;

          l_tgt_rec_count := l_tgt_rec_count + 1;
          
          -- Perform insert, upsert or delete functions
          Set_SDB_CLIENT_ATTRIBUTES(l_tgt_rec, c_trans_name);
          
        -- CH: Post Record Actions 
        -- CH: [Post Replicate]
        EXCEPTION
          WHEN e_RECORD_FAIL  THEN
            l_fail_count := l_fail_count + 1;
            g_failed_trans := TRUE;
            HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'WARN', NULL, 
                               l_key_data||' - Iteration '||iteration, 
                               'TARGET RECORD FAILURE');
          WHEN e_discard_record THEN
            l_discard_count := l_discard_count + 1;
            HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'WARN', NULL, 
                               l_key_data||' - Iteration '||iteration, 
                               'Discarding RECORD.');
        END;
    END LOOP PROCESS_CURSOR;
    
    COMMIT;    
    -- report completion status, record counts, etc
    HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, 
                       NULL, 'TRANSACTION completed'||
                       ' - SOURCE Records Processed: '||l_src_rec_count||
                       ' - Target Records Processed: '||l_tgt_rec_count||
                       ' - Discarded Records: '||l_discard_count||
                       ' - Failed Records: '||l_fail_count);
    
  EXCEPTION
    WHEN OTHERS THEN
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'ERROR', NULL,
                         l_key_data||' - Iteration '||iteration, 
                         'Transaction Failure - '||SQLERRM);
      ROLLBACK TO Collate_SP;
      RAISE e_TRANS_FAIL;
  END STAFF_CONCURRENT_ROLES;
  

  PROCEDURE STAFF_LIB_ADDR_ATTR IS
    c_trans_name CONSTANT VARCHAR2(60) := 'STAFF_LIB_ADDR_ATTR';    

    -- CH: Collate 
    CURSOR emp_cr IS
      SELECT emp.*
            ,ccr.trs_client_id AS CLIENT_ID
        FROM EMPLOYEE        emp
            ,CDS_CLIENT_ROLE ccr
       WHERE emp.employee# = ccr.role_identity(+)
         AND ccr.role_cd(+) = 'STAFF'
         AND emp.FIRST_COMMENCE  IS NOT NULL
         AND emp.TP BETWEEN TRUNC(g_RUN_START) - 2
                        AND TRUNC(g_RUN_END)
         AND EXISTS
            (SELECT *
               FROM substantive s
              WHERE s.EMPLOYEE# = emp.employee#
                AND hrm_qut_lib.EXT_SUBTERM(s.employee#, s.job#, s.commence_date)
                          > SYSDATE - 365
                AND s.classification NOT IN ('PRACT', 'SOC'));

    e_discard_record  EXCEPTION;

    l_key_data        VARCHAR2(100) := NULL;
    
    iteration         NUMBER   := NULL;
    l_src_rec_count   NUMBER   := 0;
    l_tgt_rec_count   NUMBER   := 0;
    l_discard_count   NUMBER   := 0;
    l_fail_count      NUMBER   := 0;
    
    l_tmp_rec         sdb_client_attributes%ROWTYPE := NULL;
    
    l_tgt_rec         sdb_client_attributes%ROWTYPE := NULL;
  BEGIN
    HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, 
                       NULL, 'TRANSACTION started');

    SAVEPOINT Collate_SP;

    -- CH: Collate Cursor Loop
    <<PROCESS_CURSOR>>
    FOR emp_rec IN emp_cr
    LOOP
      l_key_data := 'EMP '||emp_rec.employee#;
                    
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'DEBUG', NULL, 
                         l_key_data, 'START processing RECORD.');

      iteration       := 0;  -- for reporting purposes
      l_src_rec_count := l_src_rec_count + 1;
      BEGIN
        IF emp_rec.client_id IS NULL THEN
          HUB_LOG.LOG_WRITE 
              (gc_interface_name, c_trans_name, 'WARN', NULL, 
               l_key_data||' - Iteration '||iteration,
               'IAM has NOT created client_id FOR EMP#:'||emp_rec.employee#|| 
               ' - Discarding RECORD.');

            RAISE e_discard_record;
        END IF;

        BEGIN
          l_tmp_rec := NULL;
          SELECT *
            INTO l_tmp_rec
            FROM sdb_client_attributes   sca
           WHERE sca.client_id = emp_rec.client_id
             AND sca.attr_type = 'ROLE'
             AND sca.attr_id   = 'STAFF';
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            l_tmp_rec := NULL;
        END;

        IF l_tmp_rec.client_id IS NULL THEN
          HUB_LOG.LOG_WRITE 
              (gc_interface_name, c_trans_name, 'WARN', NULL, 
               l_key_data||' - Iteration '||iteration,
               'Staff ROLE NOT FOUND IN SDB FOR client: '||emp_rec.client_id||
               ' - Employee#: '||emp_rec.employee#|| 
               ' - Discarding RECORD.');

            RAISE e_discard_record;
        END IF;

        -- CH: [Replicate Loop]
         <<REPLICATE>>
        FOR iteration IN 1..4
        LOOP
          -- CH: Pre Record Actions 
          -- CH: Each Record Attributes 
            -- Prepare value for each field in the records and set them to a local variable
            l_tgt_rec := NULL;
            
            -- Assign target attributes

            l_tgt_rec.client_id  := emp_rec.client_id;
            
            IF iteration = 1 THEN
              l_tgt_rec.ATTR_DATA := 
                   SUBSTR(emp_rec.STREET_ADDRESS_POST||' '||
                          emp_rec.TOWN_ADDRESS_POST||' '||
                          CASE emp_rec.STATE_CODE_POST 
                            WHEN 'OTH' THEN emp_rec.ADDRESS_POST3||' '||emp_rec.ADDRESS_POST4
                            ELSE emp_rec.ADDRESS_POST3||' '||emp_rec.STATE_CODE_POST
                          END ||' $'||
                          emp_rec.POST_CODE_POST,1,79
                         );
            ELSIF iteration = 2 THEN
              l_tgt_rec.ATTR_DATA := 
                   SUBSTR(emp_rec.STREET_ADDRESS||' '||
                          emp_rec.TOWN_ADDRESS||' '||
                          CASE emp_rec.STATE_CODE 
                            WHEN 'OTH' THEN emp_rec.ADDRESS3||' '||emp_rec.ADDRESS4
                            ELSE emp_rec.ADDRESS3||' '||emp_rec.STATE_CODE
                          END ||' $'||
                          emp_rec.POST_CODE,1,79
                         );
            ELSIF iteration = 3 THEN
              l_tgt_rec.ATTR_DATA := emp_rec.HOME_PHONE#;
            ELSE
              l_tgt_rec.ATTR_DATA := emp_rec.HOME_PHONE#;
            END IF;

            IF iteration = 1 THEN 
              l_tgt_rec.ATTR_ID := 'ADDR_1';
            ELSIF iteration = 2 THEN
              l_tgt_rec.ATTR_ID := 'ADDR_2';
            ELSIF iteration = 3 THEN
              l_tgt_rec.ATTR_ID := 'PH_1';
            ELSE 
              l_tgt_rec.ATTR_ID := 'PH_2';
            END IF;
            
            
            l_tgt_rec.ATTR_TYPE := 'LIBRARY';
            
            l_tgt_rec.END_DATE  := l_tmp_rec.end_date;

            l_tgt_rec.OWN_ATTR_ID := 'STAFF';

            l_tgt_rec.OWN_ATTR_TYPE := 'ROLE';

            l_tgt_rec.own_client_id  := emp_rec.client_id;

            l_tgt_rec.START_DATE := emp_rec.FIRST_COMMENCE;

            l_tgt_rec_count := l_tgt_rec_count + 1;
            
            -- Perform insert, upsert or delete functions
            Set_SDB_CLIENT_ATTRIBUTES(l_tgt_rec, c_trans_name);
            
          -- CH: Post Record Actions 
        END LOOP REPLICATE;
        -- CH: [Post Replicate]
      EXCEPTION
        WHEN e_RECORD_FAIL  THEN
          l_fail_count := l_fail_count + 1;
          HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'WARN', NULL, 
                             l_key_data||' - Iteration '||iteration, 
                             'TARGET RECORD FAILURE');
        WHEN e_DISCARD_RECORD THEN
          l_discard_count := l_discard_count + 1;
          HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'WARN', NULL, 
                             l_key_data||' - Iteration '||iteration, 
                             'Discarding RECORD.');
      END;
    END LOOP PROCESS_CURSOR;
    
    COMMIT;    
    -- report completion status, record counts, etc
    HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, 
                       NULL, 'TRANSACTION completed'||
                       ' - SOURCE Records Processed: '||l_src_rec_count||
                       ' - Target Records Processed: '||l_tgt_rec_count||
                       ' - Discarded Records: '||l_discard_count||
                       ' - Failed Records: '||l_fail_count);
    
  EXCEPTION
    WHEN OTHERS THEN
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'ERROR', NULL,
                         l_key_data||' - Iteration '||iteration, 
                         'Transaction Failure - '||SQLERRM);
      ROLLBACK TO Collate_SP;
      RAISE e_TRANS_FAIL;
  END STAFF_LIB_ADDR_ATTR;

  
  ----------------------------------------------------------------------------
  -- MAIN_CONTROL is entry point
  ----------------------------------------------------------------------------
  PROCEDURE MAIN_CONTROL IS
    c_this_proc      CONSTANT VARCHAR2(20) := 'MAIN_CONTROL';
    l_phase          VARCHAR2(100)         := 'Initialising';
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
       l_phase := 'EXECUTE PRE_INTERFACE';
       PRE_INTERFACE;
    
       l_phase := 'EXECUTE TRANSACTIONS';

       l_phase := 'EXECUTE TRANSACTION - SDB_ORG_UNITS_TRANS';
       SDB_ORG_UNITS_TRANS;
       
       l_phase := 'EXECUTE TRANSACTION - POSITION_STRUCTURE';
       POSITION_STRUCTURE;
       
       l_phase := 'EXECUTE TRANSACTION - STAFF_CLIENTS';
       STAFF_CLIENTS;
       
       l_phase := 'EXECUTE TRANSACTION - STAFF_CLIENT_ROLES';
       STAFF_CLIENT_ROLES;

       l_phase := 'EXECUTE TRANSACTION - STAFF_IDCARD_ROLES';
       STAFF_IDCARD_ROLES;
       
       l_phase := 'EXECUTE TRANSACTION - STAFF_SUBSTANTIVE_DELETE';
       STAFF_SUBSTANTIVE_DELETE;
       
       l_phase := 'EXECUTE TRANSACTION - STAFF_HDA_DELETE';
       STAFF_HDA_DELETE;
       
       l_phase := 'EXECUTE TRANSACTION - STAFF_CONCURRENT_DELETE';
       STAFF_CONCURRENT_DELETE;
       
       l_phase := 'EXECUTE TRANSACTION - STAFF_SUBSTANTIVE_ROLES';
       STAFF_SUBSTANTIVE_ROLES;
       
       l_phase := 'EXECUTE TRANSACTION - STAFF_HDA_ROLES';
       STAFF_HDA_ROLES;
       
       l_phase := 'EXECUTE TRANSACTION - STAFF_CONCURRENT_ROLES';
       STAFF_CONCURRENT_ROLES;
       
       l_phase := 'EXECUTE TRANSACTION - STAFF_LIB_ADDR_ATTR';
       STAFF_LIB_ADDR_ATTR;
       
       --STAFF_IAS_TYPE; --- No longer required/used??
       
       l_phase := 'EXECUTE POST_INTERFACE';
       POST_INTERFACE;
    
    EXCEPTION
       -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.        
      WHEN e_TRANS_FAIL THEN
        HUB_LOG.LOG_WRITE (gc_interface_name, c_this_proc, 'ERROR', NULL, NULL, 
                           'Transaction Failure - see logs');
        g_failed_trans := TRUE;
      WHEN OTHERS THEN
        HUB_LOG.LOG_WRITE (gc_interface_name, c_this_proc, 'ERROR', NULL, NULL, 
                           'MAIN_CONTROL WHEN OTHERS Exception during Phase: '||
                           l_phase||' - '||SQLERRM);
    END;

    -- Log that this interface has finished.
    l_end_time := LOCALTIMESTAMP;
    l_elapsed_time := l_end_time - l_start_time;
    HUB_LOG.LOG_WRITE (gc_interface_name, c_this_proc, 'INFO', NULL, 
                       'Elapsed TIME ' || l_elapsed_time, 
                       'Ended AT: ' || TO_CHAR (l_end_time));
  END MAIN_CONTROL;

  
END SDB_FROM_HR;
/
