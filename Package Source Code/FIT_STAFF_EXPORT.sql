CREATE OR REPLACE PACKAGE FIT_Staff_Export AS

  PROCEDURE Main_Control;

  PROCEDURE Send_File;

END FIT_Staff_Export;
/


CREATE OR REPLACE PACKAGE BODY FIT_STAFF_EXPORT AS
   gc_interface_name    CONSTANT VARCHAR2 (20) := 'FIT_STAFF_EXPORT';
   g_run_start          TIMESTAMP;
   g_run_end            TIMESTAMP;

   e_discard_record     EXCEPTION; -- used in pre-record to skip

   PROCEDURE Collect_FIT_Staff  IS

      c_trans_name CONSTANT VARCHAR2(60) := 'COLLECT_FIT_STAFF';

      ------------------------------------------
      -- FIT Staff cursor
      ------------------------------------------                    
      CURSOR fit_staff IS
         SELECT codes.description,
                  emp.employee# emp_employee#,
                  emp.first_name,
                  emp.preferred_name,
                  emp.gender,
                  emp.surname,
                  emp.title,
                  ip.primary_campus,
                  ip.primary_location,
                  ip.primary_extn,
                  qcr.username,
                  qcca.email_alias,
                  sub.occup_term_date,
                  sub.job#,
                  sub.occup_pos_title,
                  sub.award,
                  sub.commence_date,
                  sub.employee# sub_employee#
         FROM codes, substantive sub, employee emp, -- HRM tables
              qv_client_role qcr, qv_client_computer_account qcca, ip -- QV tables
         WHERE sub.employee# = emp.employee#
         AND sub.employee# = qcr.ID
         AND qcr.username = qcca.username
         AND sub.employee# = ip.employee_num
         AND sub.clevel = codes.code
         AND codes.kind = 'CLEVEL5'
         AND sub.occup_term_date >= SYSDATE
         AND sub.classification NOT IN ('PRACT', 'SOC')
         AND (sub.clevel LIKE '146%' -- Faculty of Science
           OR sub.clevel LIKE '147%' -- Faculty of Engineering
           OR sub.clevel LIKE '137031%'
           OR sub.clevel LIKE '137032%'
           OR sub.clevel LIKE '137033%' -- Research Portfolio subsets
           OR sub.clevel LIKE '129%' -- (OLD), IFE : Institute for Future Environments
           OR sub.clevel LIKE '132%' -- (OLD), SEF : Science and Engineering Faculty
             );

   BEGIN

      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, NULL, 'TRANSACTION started');

      -- Clear temp table
      DELETE FROM FIT_STAFF_TEMP;

      <<staffloop>>
      FOR staff_rec IN fit_staff
      LOOP
         DECLARE
            l_phase          VARCHAR2(50)          := 'Initialising';

            var_SB_COMM_DT    VARCHAR2(22)                        := NULL;
            var_SB_EMP        substantive.employee#%TYPE          := staff_rec.sub_employee#;
            var_SB_JOB        substantive.job#%TYPE               := staff_rec.JOB#;       
            var_CARDAX_NUM    id_card.cardax_number%TYPE          := NULL;
            var_CARDAX_ISS    id_card.cardax_issue_level%TYPE     := NULL;

            l_fit_staff_rec         FIT_STAFF_TEMP%ROWTYPE;
         BEGIN  

            ------------------------------------------
            -- Constellar Hub : Pre Record Actions 
            ------------------------------------------
            l_phase := 'Commence date';
            SELECT TO_CHAR(MAX(sb.commence_date),'DD-MON-YYYY HH24:MI:SS')
            INTO var_SB_COMM_DT
            FROM substantive sb
            WHERE sb.employee# = var_SB_EMP
            AND sb.job# = var_SB_JOB
            and sb.classification NOT IN ('PRACT', 'SOC')
            AND sb.occup_term_date > SYSDATE;


            IF staff_rec.COMMENCE_DATE != TO_DATE(var_SB_COMM_DT,'DD-MON-YYYY HH24:MI:SS') THEN
               RAISE e_discard_record;
            END IF;

            l_phase := 'Cardax details';
            BEGIN
               SELECT a.cardax_number, 
                       a.cardax_issue_level
               INTO var_CARDAX_NUM, 
                     var_CARDAX_ISS
               FROM id_card a
               WHERE a.role_id = var_SB_EMP
               AND a.role_cd = 'STAFF'
               AND a.issue_level = (SELECT MAX(b.issue_level)
                                    FROM id_card b
                                    WHERE b.role_cd = 'STAFF'
                                    AND b.role_id = a.role_id
                                    AND b.issue_dt <= SYSDATE);
            EXCEPTION
               WHEN OTHERS THEN
                  VAR_CARDAX_NUM := NULL;
                  VAR_CARDAX_ISS := NULL;
            END; 

            ------------------------------------------
            -- Constellar Hub : Each Record Actions
            ------------------------------------------
            l_phase := 'Set record data';
            l_fit_staff_rec.ACCESS_ID := staff_rec.USERNAME;
            l_fit_staff_rec.CAMPUS := staff_rec.PRIMARY_CAMPUS;
            l_fit_staff_rec.CARDAX_ISSUE := var_CARDAX_ISS;
            l_fit_staff_rec.CARDAX_NUM := var_CARDAX_NUM;
            l_fit_staff_rec.EMAIL_ADDRESS := staff_rec.EMAIL_ALIAS;
            l_fit_staff_rec.EMPLOYEE_NUM := staff_rec.emp_EMPLOYEE#;
            l_fit_staff_rec.END_DATE := staff_rec.OCCUP_TERM_DATE;
            l_fit_staff_rec.FIRST_NAME := staff_rec.FIRST_NAME;
            l_fit_staff_rec.JOB_NUM := staff_rec.JOB#;
            l_fit_staff_rec.JOB_TITLE := SUBSTR(staff_rec.OCCUP_POS_TITLE,1,40);
            l_fit_staff_rec.LOCATION := staff_rec.PRIMARY_LOCATION;
            l_fit_staff_rec.PHONE := SUBSTR(staff_rec.PRIMARY_EXTN,1,20);
            l_fit_staff_rec.PREFERRED_NAME := staff_rec.PREFERRED_NAME;
            l_fit_staff_rec.SCHOOL_CODE := LTRIM(staff_rec.DESCRIPTION);
            l_fit_staff_rec.SEX := staff_rec.GENDER;
            l_fit_staff_rec.STAFF_TYPE := staff_rec.AWARD;
            l_fit_staff_rec.START_DATE := staff_rec.COMMENCE_DATE;
            l_fit_staff_rec.SURNAME := staff_rec.SURNAME;
            l_fit_staff_rec.TITLE := staff_rec.TITLE;

            ------------------------------------------
            -- Constellar Hub : Actions = Insert, Update or Delete
            ------------------------------------------
            l_phase := 'Insert record';
            INSERT INTO FIT_STAFF_TEMP VALUES l_fit_staff_rec;
            COMMIT;

         EXCEPTION
            WHEN e_discard_record THEN
               NULL; -- continue to next record
            WHEN OTHERS THEN
               HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'ERROR', NULL, 
                                    'username:'||staff_rec.USERNAME||
                                    ' emp#:'||staff_rec.emp_EMPLOYEE#||
                                    ' job#:'||staff_rec.JOB#,
                                    'Transaction failure during Phase: '||l_phase||' - '||SQLERRM);
         END;
      END LOOP staffloop;

      -- report completion status
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, 
                       NULL, 'TRANSACTION completed');

   EXCEPTION
      WHEN OTHERS THEN
         HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'ERROR', NULL, NULL, SQLERRM);
         RAISE;                       
   END Collect_FIT_Staff;


   ----------------------------------------------------------------------------
   -- Send_File - Creates and sends the flat file to FIT 
   ----------------------------------------------------------------------------
   PROCEDURE Send_File IS
      c_trans_name CONSTANT VARCHAR2(60) := 'SEND_FILE';
      l_staff_file         FFA.FFA_File := NULL;
   BEGIN
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, NULL, 'TRANSACTION started');

      l_staff_file := FFA.New_File('FIT_STAFF', NULL);

      --collate elements
      FOR staff_rec IN 
         (SELECT *
         FROM FIT_STAFF_TEMP)
      LOOP
         FFA.New_Record(l_staff_file);

         FFA.Add_Record_Element(l_staff_file, 'ACCESS_ID' ,staff_rec.ACCESS_ID);
         FFA.Add_Record_Element(l_staff_file, 'CAMPUS' ,staff_rec.CAMPUS);
         FFA.Add_Record_Element(l_staff_file, 'CARDAX_ISSUE' ,staff_rec.CARDAX_ISSUE);
         FFA.Add_Record_Element(l_staff_file, 'CARDAX_NUM' ,staff_rec.CARDAX_NUM);
         FFA.Add_Record_Element(l_staff_file, 'EMAIL_ADDRESS' ,staff_rec.EMAIL_ADDRESS);
         FFA.Add_Record_Element(l_staff_file, 'EMPLOYEE_NUM' ,staff_rec.EMPLOYEE_NUM);
         FFA.Add_Record_Element(l_staff_file, 'END_DATE' ,TO_CHAR(staff_rec.END_DATE,'DD/MM/YYYY'));
         FFA.Add_Record_Element(l_staff_file, 'FIRST_NAME' ,staff_rec.FIRST_NAME);
         FFA.Add_Record_Element(l_staff_file, 'JOB_NUM' ,staff_rec.JOB_NUM);
         FFA.Add_Record_Element(l_staff_file, 'JOB_TITLE' ,staff_rec.JOB_TITLE);
         FFA.Add_Record_Element(l_staff_file, 'LOCATION' ,staff_rec.LOCATION);
         FFA.Add_Record_Element(l_staff_file, 'PHONE' ,staff_rec.PHONE);
         FFA.Add_Record_Element(l_staff_file, 'PREFERRED_NAME' ,staff_rec.PREFERRED_NAME);
         FFA.Add_Record_Element(l_staff_file, 'SCHOOL_CODE' ,staff_rec.SCHOOL_CODE);
         FFA.Add_Record_Element(l_staff_file, 'SEX' ,staff_rec.SEX);
         FFA.Add_Record_Element(l_staff_file, 'STAFF_TYPE' ,staff_rec.STAFF_TYPE);
         FFA.Add_Record_Element(l_staff_file, 'START_DATE' ,TO_CHAR(staff_rec.START_DATE,'DD/MM/YYYY'));
         FFA.Add_Record_Element(l_staff_file, 'SURNAME' ,staff_rec.SURNAME);
         FFA.Add_Record_Element(l_staff_file, 'TITLE' ,staff_rec.TITLE);

      END LOOP;

      -- close file (includes send)      
      FFA.Close_File(l_staff_file);

      -- report completion status
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, 
                       NULL, 'TRANSACTION completed');

   EXCEPTION
      WHEN FFA.e_FFA_CREATE_FAILURE THEN
         HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'ERROR', NULL,NULL,                           
                         'Transaction Failure - FFA File Creation Failure - '||SQLERRM);
      WHEN FFA.e_FFA_HANDLER_FAILURE THEN              
         HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'ERROR', NULL,NULL,                           
                         'Transaction Failure - FFA File Handler Failure - '||SQLERRM);
      WHEN FFA.e_FFA_SEND_FAILURE THEN              
         HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'ERROR', NULL,NULL,                           
                         'Transaction Failure - FFA File Send Failure - '||SQLERRM);
      WHEN OTHERS THEN
         HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'ERROR', NULL,NULL,                           
                         'Transaction Failure - '||SQLERRM);

   END Send_File;


   PROCEDURE MAIN_CONTROL IS
      c_this_proc      CONSTANT VARCHAR2(20) := 'MAIN_CONTROL';
      l_phase          VARCHAR2(50)          := 'Initialising';
      l_start_time     TIMESTAMP;
      l_end_time       TIMESTAMP;
      l_elapsed_time   INTERVAL DAY (2) TO SECOND (6);    
   BEGIN
      -- Log that this interface has started.
      l_start_time := LOCALTIMESTAMP;
      HUB_LOG.LOG_WRITE (gc_interface_name, c_this_proc, 'INFO', NULL, 
                        'Starting ' || gc_interface_name, 
                        'Start at: ' || TO_CHAR (l_start_time));

      BEGIN

         l_phase := 'Collect FIT staff';
         Collect_FIT_Staff();

         l_phase := 'Create and send file';
         Send_File();

      EXCEPTION
         -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.        
         WHEN OTHERS THEN
            HUB_LOG.LOG_WRITE (gc_interface_name, c_this_proc, 'ERROR', NULL, 
            'MAIN_CONTROL WHEN OTHERS EXCEPTION during Phase: '||l_phase,
            SQLERRM);
      END;

      -- Log that this interface has finished.
      l_end_time := LOCALTIMESTAMP;
      l_elapsed_time := l_end_time - l_start_time;
      HUB_LOG.LOG_WRITE (gc_interface_name, c_this_proc, 'INFO', NULL, 
                        'Elapsed time ' || l_end_time, 
                        'Ended at: ' || TO_CHAR (l_end_time));
   END MAIN_CONTROL;

END FIT_STAFF_EXPORT;

/
