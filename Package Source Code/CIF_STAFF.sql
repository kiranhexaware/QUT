CREATE OR REPLACE PACKAGE CIF_STAFF
AS
   PROCEDURE MAIN_CONTROL;
END CIF_STAFF;
/


CREATE OR REPLACE PACKAGE BODY CIF_STAFF
AS
   GC_INTERFACE_NAME   CONSTANT VARCHAR2(20) := 'CIF_STAFF';
   G_RUN_START                  TIMESTAMP;
   G_RUN_END                    TIMESTAMP;

   PROCEDURE PRE_INTERFACE
   IS
   BEGIN
      NULL;
   END PRE_INTERFACE;

   PROCEDURE EXPORT_CIF_STAFFS
   IS
      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'EXPORT_CIF_STAFFS';
      VAR_TABLE_NAME         VARCHAR2(4000) := NULL;
      VAR_KEY_DATA           VARCHAR2(4000) := NULL;

      L_REC_COUNT            NUMBER := 0;
      L_CIF_STAFFS_FILE      FFA.FFA_FILE := NULL;
   BEGIN
      L_CIF_STAFFS_FILE := FFA.NEW_FILE('CIF_STAFFS', NULL);

      FOR STAFFS_REC IN (SELECT * FROM TMP_CIF_STAFF)
      LOOP
         -- write records to file
         L_REC_COUNT :=
            L_REC_COUNT
            + 1;

         VAR_KEY_DATA :=
               'EMPLOYEE_NUM:'
            || STAFFS_REC.ACCESS_ID
            || '|'
            || 'CARDAX_NUM:'
            || STAFFS_REC.CARDAX_NUM
            || '|'
            || 'EMPLOYEE_NUM:'
            || STAFFS_REC.EMPLOYEE_NUM
            || '|'
            || 'JOB_NUM:'
            || STAFFS_REC.JOB_NUM;

         FFA.NEW_RECORD(L_CIF_STAFFS_FILE);
         FFA.ADD_RECORD_ELEMENT(L_CIF_STAFFS_FILE, 'ACCESS_ID', STAFFS_REC.ACCESS_ID);
         FFA.ADD_RECORD_ELEMENT(L_CIF_STAFFS_FILE, 'CAMPUS', STAFFS_REC.CAMPUS);
         FFA.ADD_RECORD_ELEMENT(L_CIF_STAFFS_FILE, 'CARDAX_ISSUE', STAFFS_REC.CARDAX_ISSUE);
         FFA.ADD_RECORD_ELEMENT(L_CIF_STAFFS_FILE, 'CARDAX_NUM', STAFFS_REC.CARDAX_NUM);
         FFA.ADD_RECORD_ELEMENT(L_CIF_STAFFS_FILE, 'EMAIL_ADDRESS', STAFFS_REC.EMAIL_ADDRESS);
         FFA.ADD_RECORD_ELEMENT(L_CIF_STAFFS_FILE, 'EMPLOYEE_NUM', STAFFS_REC.EMPLOYEE_NUM);
         FFA.ADD_RECORD_ELEMENT(L_CIF_STAFFS_FILE, 'END_DATE', TO_CHAR(STAFFS_REC.END_DATE, 'DD/MM/YYYY'));
         FFA.ADD_RECORD_ELEMENT(L_CIF_STAFFS_FILE, 'FIRST_NAME', STAFFS_REC.FIRST_NAME);
         FFA.ADD_RECORD_ELEMENT(L_CIF_STAFFS_FILE, 'JOB_NUM', STAFFS_REC.JOB_NUM);
         FFA.ADD_RECORD_ELEMENT(L_CIF_STAFFS_FILE, 'JOB_TITLE', STAFFS_REC.JOB_TITLE);
         FFA.ADD_RECORD_ELEMENT(L_CIF_STAFFS_FILE, 'LOCATION', STAFFS_REC.LOCATION);
         FFA.ADD_RECORD_ELEMENT(L_CIF_STAFFS_FILE, 'PHONE', STAFFS_REC.PHONE);
         FFA.ADD_RECORD_ELEMENT(L_CIF_STAFFS_FILE, 'PREFERRED_NAME', STAFFS_REC.PREFERRED_NAME);
         FFA.ADD_RECORD_ELEMENT(L_CIF_STAFFS_FILE, 'SCHOOL_CODE', STAFFS_REC.SCHOOL_CODE);
         FFA.ADD_RECORD_ELEMENT(L_CIF_STAFFS_FILE, 'SEX', STAFFS_REC.SEX);
         FFA.ADD_RECORD_ELEMENT(L_CIF_STAFFS_FILE, 'STAFF_TYPE', STAFFS_REC.STAFF_TYPE);
         FFA.ADD_RECORD_ELEMENT(L_CIF_STAFFS_FILE, 'START_DATE', TO_CHAR(STAFFS_REC.START_DATE, 'DD/MM/YYYY'));
         FFA.ADD_RECORD_ELEMENT(L_CIF_STAFFS_FILE, 'SURNAME', STAFFS_REC.SURNAME);
         FFA.ADD_RECORD_ELEMENT(L_CIF_STAFFS_FILE, 'TITLE', STAFFS_REC.TITLE);
         FFA.ADD_RECORD_ELEMENT(L_CIF_STAFFS_FILE, 'BARCODE', STAFFS_REC.BARCODE);
      END LOOP;

      -- close file and send
      FFA.CLOSE_FILE(L_CIF_STAFFS_FILE);
   EXCEPTION
      WHEN FFA.E_FFA_CREATE_FAILURE
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                           VAR_TRANSACTION_NAME,
                           'ERROR',
                           VAR_TABLE_NAME,
                           NULL,
                           'Trapped FFA.e_FFA_CREATE_FAILURE exception '
                           || SQLERRM);
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                           VAR_TRANSACTION_NAME,
                           'ERROR',
                           VAR_TABLE_NAME,
                           VAR_KEY_DATA,
                           'RECORD ADDS WHEN OTHERS EXCEPTION '
                           || SQLERRM);
   END EXPORT_CIF_STAFFS;


   PROCEDURE CIF_STAFF_EXPORT
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR CIF_CURSOR
      IS
         SELECT SUBSTANTIVE.EMPLOYEE#, SUBSTANTIVE.JOB#, SUBSTANTIVE.CLEVEL,
                QV_CLIENT_ROLE.TRS_CLIENT_ID, SUBSTANTIVE.COMMENCE_DATE,
                QV_CLIENT_COMPUTER_ACCOUNT.USERNAME, IP.PRIMARY_CAMPUS,
                QV_CLIENT_COMPUTER_ACCOUNT.CHOSEN_EMAIL, OCCUP_TERM_DATE,
                OCCUP_POS_TITLE, PRIMARY_LOCATION, EMPLOYEE.FIRST_NAME,
                PRIMARY_EXTN, EMPLOYEE.PREFERRED_NAME, CODES.DESCRIPTION,
                EMPLOYEE.GENDER, AWARD, EMPLOYEE.SURNAME, EMPLOYEE.TITLE
           FROM SUBSTANTIVE, QV_CLIENT_ROLE, QV_CLIENT_COMPUTER_ACCOUNT, IP,
                EMPLOYEE, CODES
          WHERE     SUBSTANTIVE.EMPLOYEE# = EMPLOYEE.EMPLOYEE#
                AND SUBSTANTIVE.CLEVEL = CODES.CODE
                AND CODES.KIND = 'CLEVEL5'
                AND SUBSTANTIVE.OCCUP_TERM_DATE >= SYSDATE
                AND (SUBSTR(SUBSTANTIVE.CLEVEL, 1, 3) = '121' OR 
				     SUBSTR(SUBSTANTIVE.CLEVEL, 1, 3) = '148' OR
					 SUBSTANTIVE.CLEVEL IN ('1470400', '1470221', -- School of Archi & Built Env 
						                    '1170800', '1170806', '1170807',
											'1130010',
											'1130021',
											'1130040','1130041','1130042',
											'1130050','1130053',
											'1130070','1130071',
											'1130400',
											'1130800')
			        )
                AND TO_NUMBER(EMPLOYEE.EMPLOYEE#) = QV_CLIENT_ROLE.ID
                AND QV_CLIENT_ROLE.ROLE_CD = 'EMP'
                AND QV_CLIENT_ROLE.ROLE_ACTIVE_IND = 'Y'
                AND QV_CLIENT_ROLE.USERNAME =
                    QV_CLIENT_COMPUTER_ACCOUNT.USERNAME
                AND QV_CLIENT_ROLE.TRS_CLIENT_ID = IP.IP_NUM
                AND SUBSTANTIVE.CLASSIFICATION NOT IN ('PRACT', 'SOC');

      VAR_STATUS             VARCHAR2(4000);
      VAR_KEY_DATA           VARCHAR2(4000);
      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'CIF_STAFF_EXPORT';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'TMP_CIF_FILES';
      VAR_SB_COMM_DT         VARCHAR2(4000);
      VAR_SB_EMP             VARCHAR2(4000);
      VAR_SB_JOB             VARCHAR2(4000);
      VAR_CARDAX_NUM         VARCHAR2(4000);
      VAR_CARDAX_ISS         VARCHAR2(4000);

      VAR_ACCESS_ID          VARCHAR2(4000);
      VAR_CAMPUS             VARCHAR2(4000);
      VAR_EMAIL_ADDRESS      VARCHAR2(4000);
      VAR_EMPLOYEE_NUM       VARCHAR2(4000);
      VAR_END_DATE           VARCHAR2(4000);
      VAR_FIRST_NAME         VARCHAR2(4000);
      VAR_JOB_NUM            VARCHAR2(4000);
      VAR_JOB_TITLE          VARCHAR2(4000);
      VAR_LOCATION           VARCHAR2(4000);
      VAR_PHONE              VARCHAR2(4000);
      VAR_PREFERRED_NAME     VARCHAR2(4000);
      VAR_SCHOOL_CODE        VARCHAR2(4000);
      VAR_SEX                VARCHAR2(4000);
      VAR_STAFF_TYPE         VARCHAR2(4000);
      VAR_START_DATE         VARCHAR2(4000);
      VAR_SURNAME            VARCHAR2(4000);
      VAR_TITLE              VARCHAR2(4000);
      VAR_SB_MAX_JOB         VARCHAR2(4000);
      VAR_BARCODE            VARCHAR2(4000);
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Transaction Actions
      ------------------------------------------
      DELETE TMP_CIF_STAFF;

      FOR CIF_REC IN CIF_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : Pre Record Actions
         ------------------------------------------
         BEGIN
            VAR_SB_COMM_DT := NULL;
            VAR_SB_EMP := CIF_REC.EMPLOYEE#;
            VAR_SB_JOB := CIF_REC.JOB#;
            VAR_CARDAX_NUM := NULL;
            VAR_CARDAX_ISS := NULL;
            VAR_SB_COMM_DT := NULL;
            VAR_BARCODE := NULL;
            VAR_KEY_DATA :=
                  'EMPLOYEE#:'
               || CIF_REC.EMPLOYEE#
               || '|'
               || 'JOB#:'
               || CIF_REC.JOB#
               || '|'
               || 'CLEVEL:'
               || CIF_REC.CLEVEL
               || '|'
               || 'TRS_CLIENT_ID:'
               || CIF_REC.TRS_CLIENT_ID;

            SELECT TO_CHAR(MAX(SB.COMMENCE_DATE), 'DD-MON-YYYY HH24:MI:SS')
              INTO VAR_SB_COMM_DT
              FROM SUBSTANTIVE SB
             WHERE     SB.EMPLOYEE# = VAR_SB_EMP
                   AND SB.JOB# = VAR_SB_JOB
                   AND SB.OCCUP_TERM_DATE > SYSDATE
                   AND SB.CLASSIFICATION NOT IN ('PRACT', 'SOC');

            SELECT MAX(JOB#)
              INTO VAR_SB_MAX_JOB
              FROM SUBSTANTIVE SB
            WHERE     SB.EMPLOYEE# = VAR_SB_EMP
                   AND (SUBSTR(SB.CLEVEL, 1, 3) = '121' OR
				        SUBSTR(SB.CLEVEL, 1, 3) = '148' OR
					    SB.CLEVEL IN ('1470400', '1470221', -- School of Archi & Built Env 
						                    '1170800', '1170806', '1170807',
											'1130010',
											'1130021',
											'1130040','1130041','1130042',
											'1130050','1130053',
											'1130070','1130071',
											'1130400',
											'1130800')
                        )
                   AND SB.OCCUP_TERM_DATE > SYSDATE
                   AND SB.CLASSIFICATION NOT IN ('PRACT', 'SOC');

            IF (CIF_REC.COMMENCE_DATE !=
                   TO_DATE(VAR_SB_COMM_DT, 'DD-MON-YYYY HH24:MI:SS')
                OR CIF_REC.JOB# != VAR_SB_MAX_JOB)
            THEN
               -- DIscard the record
               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, 'RECORD DISCARDED');
            ELSE
               BEGIN
                  SELECT a.CARDAX_NUMBER, a.CARDAX_ISSUE_LEVEL, REPLACE(a.FRONT_BARCODE, ' ') AS BARCODE
                    INTO VAR_CARDAX_NUM, VAR_CARDAX_ISS, VAR_BARCODE
                    FROM ID_CARD a
                   WHERE     a.ROLE_ID = VAR_SB_EMP
                         AND a.ROLE_CD = 'STAFF'
                         AND a.ISSUE_LEVEL = (SELECT MAX(b.ISSUE_LEVEL)
                                                FROM ID_CARD b
                                               WHERE     b.ROLE_CD = 'STAFF'
                                                     AND b.ROLE_ID = a.ROLE_ID
                                                     AND b.ISSUE_DT <= SYSDATE);
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     VAR_CARDAX_NUM := NULL;
                     VAR_CARDAX_ISS := NULL;
                     VAR_BARCODE := NULL;
               END;

               ------------------------------------------
               -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
               ------------------------------------------
               ------------------------------------------
               -- Constellar Hub : Each Record Actions
               ------------------------------------------
               -- Prepare value for each field in the records and set them to a local variable
               VAR_STATUS := NULL;
               VAR_ACCESS_ID := CIF_REC.USERNAME;
               VAR_CAMPUS := CIF_REC.PRIMARY_CAMPUS;
               VAR_EMAIL_ADDRESS := CIF_REC.CHOSEN_EMAIL;
               VAR_EMPLOYEE_NUM := CIF_REC.EMPLOYEE#;
               VAR_END_DATE := CIF_REC.OCCUP_TERM_DATE;
               VAR_FIRST_NAME := SUBSTR(CIF_REC.FIRST_NAME, 1, 22);
               VAR_JOB_NUM := CIF_REC.JOB#;
               VAR_JOB_TITLE := SUBSTR(CIF_REC.OCCUP_POS_TITLE, 1, 40);
               VAR_LOCATION := CIF_REC.PRIMARY_LOCATION;
               VAR_PHONE := SUBSTR(CIF_REC.PRIMARY_EXTN, 1, 20);
               VAR_PREFERRED_NAME := SUBSTR(CIF_REC.PREFERRED_NAME, 1, 22);
               VAR_SCHOOL_CODE := LTRIM(CIF_REC.DESCRIPTION);
               VAR_SEX := CIF_REC.GENDER;
               VAR_STAFF_TYPE := CIF_REC.AWARD;
               VAR_START_DATE := CIF_REC.COMMENCE_DATE;
               VAR_SURNAME := SUBSTR(CIF_REC.SURNAME, 1, 22);
               VAR_TITLE := CIF_REC.TITLE;

               ------------------------------------------
               -- Constellar Hub : Actions = Insert, Update or Delete
               ------------------------------------------
               -- Perform insert, upsert or delete functions
               BEGIN
                  INSERT INTO TMP_CIF_STAFF(ACCESS_ID, CAMPUS, CARDAX_ISSUE,
                                 CARDAX_NUM, EMAIL_ADDRESS, EMPLOYEE_NUM,
                                 END_DATE, FIRST_NAME, JOB_NUM, JOB_TITLE,
                                 LOCATION, PHONE, PREFERRED_NAME, SCHOOL_CODE,
                                 SEX, STAFF_TYPE, START_DATE, SURNAME, TITLE, BARCODE)
                       VALUES (VAR_ACCESS_ID, VAR_CAMPUS, VAR_CARDAX_ISS,
                               VAR_CARDAX_NUM, VAR_EMAIL_ADDRESS,
                               VAR_EMPLOYEE_NUM, VAR_END_DATE, VAR_FIRST_NAME,
                               VAR_JOB_NUM, VAR_JOB_TITLE, VAR_LOCATION,
                               VAR_PHONE, VAR_PREFERRED_NAME, VAR_SCHOOL_CODE,
                               VAR_SEX, VAR_STAFF_TYPE, VAR_START_DATE,
                               VAR_SURNAME, VAR_TITLE, VAR_BARCODE);

                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'DEBUG',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'INSERT'
                                    || SQLERRM);
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                       VAR_TRANSACTION_NAME,
                                       'ERROR',
                                       VAR_TABLE_NAME,
                                       VAR_KEY_DATA,
                                       'RECORD INSERT WHEN OTHERS EXCEPTION '
                                       || SQLERRM);
                     RAISE;
               END;
            END IF;
         END;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      -- Unique per source
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'WHEN OTHERS EXCEPTION', SQLERRM);
         ROLLBACK;
   END CIF_STAFF_EXPORT;

   PROCEDURE POST_INTERFACE
   IS
   BEGIN
      NULL;
   END POST_INTERFACE;

   PROCEDURE MAIN_CONTROL
   IS
      C_THIS_PROC   CONSTANT VARCHAR2(20) := 'MAIN_CONTROL';
      L_PHASE                VARCHAR2(50) := 'Initialising';
      L_START_TIME           TIMESTAMP;
      L_END_TIME             TIMESTAMP;
      L_ELAPSED_TIME         INTERVAL DAY(2) TO SECOND(6);
   BEGIN
      -- Log that this interface has started.
      L_START_TIME := LOCALTIMESTAMP;
      HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                        C_THIS_PROC,
                        'INFO',
                        NULL,
                        'Starting '
                        || GC_INTERFACE_NAME,
                        'Start at: '
                        || TO_CHAR(L_START_TIME));

      BEGIN
         L_PHASE := 'EXECUTE TRANSACTIONS';
         CIF_STAFF_EXPORT;
         EXPORT_CIF_STAFFS;
      EXCEPTION
         WHEN OTHERS
         THEN
            HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                              C_THIS_PROC,
                              'ERROR',
                              NULL,
                              'MAIN_CONTROL WHEN OTHERS EXCEPTION during Phase: '
                              || L_PHASE,
                              SQLERRM);
      END;

      -- Log that this interface has finished.
      L_END_TIME := LOCALTIMESTAMP;
      L_ELAPSED_TIME :=
         L_END_TIME
         - L_START_TIME;
      HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                        C_THIS_PROC,
                        'INFO',
                        NULL,
                        'Elapsed time '
                        || L_ELAPSED_TIME,
                        'Ended at: '
                        || TO_CHAR(L_END_TIME));
   END MAIN_CONTROL;
END CIF_STAFF;
/
