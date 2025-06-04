CREATE OR REPLACE PACKAGE TRIM_FOLDER AS

PROCEDURE MAIN_CONTROL;
                               
END TRIM_FOLDER;
/


CREATE OR REPLACE PACKAGE BODY TRIM_FOLDER
AS
   GC_INTERFACE_NAME   CONSTANT VARCHAR2(20) := 'TRIM_FOLDER';
   G_RUN_START                  TIMESTAMP;
   G_RUN_END                    TIMESTAMP;

   PROCEDURE PRE_INTERFACE
   IS
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Interface Rule
      ------------------------------------------
      --      global_INTERFACE_NAME := NULL;
      --      global_RUNID := NULL;
      --      global_JOBID := NULL;
      --      global_RUN_START := NULL;
      --      global_RUN_END := NULL;

      BEGIN
         HUB_LIB.GET_RUN_DATES(GC_INTERFACE_NAME, G_RUN_START, G_RUN_END);
      EXCEPTION
         -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
         WHEN OTHERS
         THEN
            HUB_LIB.GET_RUN_DATES(GC_INTERFACE_NAME, G_RUN_START, G_RUN_END);
      END;
   END PRE_INTERFACE;

   PROCEDURE TRIM_PAST_STAFF
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR FOLDER_CURSOR
      IS
         SELECT EMPLOYEE.EMPLOYEE#, EMPLOYEE.DATE_OF_BIRTH, OCCUP_TERM_DATE,
                EMAIL_ALIAS, SURNAME, USERNAME, FIRST_NAME, NVL(EMPLOYEE.PREFERRED_NAME, EMPLOYEE.FIRST_NAME) AS PREFERRED_NAME,
                SECOND_NAME, THIRD_NAME, GENDER, PRIMARY_EXTN, INITIALS,
                IP_UPDATE_ON, QCR_UPDATE_ON, QCCR_UPDATE_ON, BARCODE,
                FIRST_COMMENCE, TITLE
           FROM EMPLOYEE, V_TRIM_PAST_STAFF_DETAILS
          WHERE EMPLOYEE.EMPLOYEE# = V_TRIM_PAST_STAFF_DETAILS.EMPLOYEE#;

      VAR_STATUS             VARCHAR2(4000);
      VAR_CLASSFN            VARCHAR2(4000);
      VAR_EMP_STATUS         VARCHAR2(4000);
      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'TRIM_PAST_STAFF';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'TRIM_PAST_STAFF';
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR FOLDER_REC IN FOLDER_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_EMP_STATUS := NULL;

         -- Check the employee status in HR
         BEGIN
            SELECT EMP_STATUS
              INTO VAR_EMP_STATUS
              FROM (SELECT EMPLOYEE#, EMP_STATUS,
                           DECODE(EMP_STATUS,  'OFT', 1,  'OPT', 2,  'FFT', 3,  'FPT', 4,  'CASG', 5,  'CASA', 6,  'VISIT', 7,  8) AS NUM
                      FROM SUBSTANTIVE
                     WHERE EMPLOYEE# = FOLDER_REC.EMPLOYEE#
                    UNION
                    SELECT EMPLOYEE#, EMP_STATUS,
                           DECODE(EMP_STATUS,  'OFT', 1,  'OPT', 2,  'FFT', 3,  'FPT', 4,  'CASG', 5,  'CASA', 6,  'VISIT', 7,  8) AS NUM
                      FROM CONCURRENT
                     WHERE EMPLOYEE# = FOLDER_REC.EMPLOYEE#
                    ORDER BY NUM)
             WHERE ROWNUM = 1;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               VAR_EMP_STATUS := NULL;
         END;

         VAR_CLASSFN := NULL;

         -- Check if the record existed and same in Trim
         BEGIN
            SELECT TRIM(AWARD || ' ' || CLASSIFICATION)
              INTO VAR_CLASSFN
              FROM (SELECT EMPLOYEE#, CLASSIFICATION, AWARD,
                           DECODE(AWARD,  'OVER', 1,  'SSGA', 2,  'ASA', 3) AS AWARD_NUM
                      FROM SUBSTANTIVE
                     WHERE EMPLOYEE# = FOLDER_REC.EMPLOYEE#
                           AND ((AWARD = 'OVER')
                                OR (AWARD = 'SSGA'
                                    AND (   CLASSIFICATION LIKE '04%'
                                         OR CLASSIFICATION LIKE '05%'
                                         OR CLASSIFICATION LIKE '06%'))
                                OR (AWARD = 'ASA'
                                    AND (CLASSIFICATION = 'LEVD'
                                         OR CLASSIFICATION = 'LEVE')))
                    UNION
                    SELECT EMPLOYEE#, CLASSIFICATION, AWARD,
                           DECODE(AWARD,  'OVER', 1,  'SSGA', 2,  'ASA', 3) AS AWARD_NUM
                      FROM CONCURRENT
                     WHERE EMPLOYEE# = FOLDER_REC.EMPLOYEE#
                           AND ((AWARD = 'OVER')
                                OR (AWARD = 'SSGA'
                                    AND (   CLASSIFICATION LIKE '04%'
                                         OR CLASSIFICATION LIKE '05%'
                                         OR CLASSIFICATION LIKE '06%'))
                                OR (AWARD = 'ASA'
                                    AND (CLASSIFICATION = 'LEVD'
                                         OR CLASSIFICATION = 'LEVE')))
                    ORDER BY AWARD_NUM, CLASSIFICATION DESC)
             WHERE ROWNUM = 1;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               VAR_CLASSFN := NULL;
         END;

         BEGIN
            VAR_STATUS := NULL;

            SELECT 'MODIFIED'
              INTO VAR_STATUS
              FROM TRIM_PAST_STAFF
             WHERE EMPLOYEE# = FOLDER_REC.EMPLOYEE#;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               VAR_STATUS := 'CREATE';
         END;

         ------------------------------------------
         -- Constellar Hub : Actions = Insert, Update or Delete
         ------------------------------------------
         -- Perform insert, upsert or delete functions
         IF VAR_STATUS = 'CREATE'
         THEN
            BEGIN
               INSERT INTO TRIM_PAST_STAFF(BARCODE, CANCEL_DT, CREATED_DT,
                              DATE_OF_BIRTH, EMAIL, EMPLOYEE#, END_DT,
                              FIRST_NAME, GENDER, HIGHEST_CLASSIFICATION,
                              HIGHEST_EMP_STATUS, INITIALS, MODIFIED_DT,
                              PHONE_EXTN, PREFERRED_NAME, SECOND_NAME,
                              START_DT, SURNAME, THIRD_NAME, TITLE, USERNAME)
                    VALUES (FOLDER_REC.BARCODE, NULL, SYSDATE, FOLDER_REC.DATE_OF_BIRTH,
                            FOLDER_REC.EMAIL_ALIAS, FOLDER_REC.EMPLOYEE#,
                            FOLDER_REC.OCCUP_TERM_DATE, FOLDER_REC.FIRST_NAME,
                            FOLDER_REC.GENDER, VAR_CLASSFN, VAR_EMP_STATUS,
                            FOLDER_REC.INITIALS, NULL, FOLDER_REC.PRIMARY_EXTN,
                            FOLDER_REC.PREFERRED_NAME, FOLDER_REC.SECOND_NAME,
                            FOLDER_REC.FIRST_COMMENCE, FOLDER_REC.SURNAME,
                            FOLDER_REC.THIRD_NAME, FOLDER_REC.TITLE,
                            FOLDER_REC.USERNAME);

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, 'INSERT', FOLDER_REC.EMPLOYEE#);
            EXCEPTION
               -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'RECORD INSERT WHEN OTHERS EXCEPTION', SQLERRM);
            END;
         ELSE
            BEGIN
               UPDATE TRIM_PAST_STAFF
                  SET BARCODE = FOLDER_REC.BARCODE, CANCEL_DT = NULL,
                      CREATED_DT = SYSDATE,
                      DATE_OF_BIRTH = FOLDER_REC.DATE_OF_BIRTH,
                      EMAIL = FOLDER_REC.EMAIL_ALIAS,
                      EMPLOYEE# = FOLDER_REC.EMPLOYEE#,
                      END_DT = FOLDER_REC.OCCUP_TERM_DATE,
                      FIRST_NAME = FOLDER_REC.FIRST_NAME,
                      GENDER = FOLDER_REC.GENDER,
                      HIGHEST_CLASSIFICATION = VAR_CLASSFN,
                      HIGHEST_EMP_STATUS = VAR_EMP_STATUS,
                      INITIALS = FOLDER_REC.INITIALS, MODIFIED_DT = NULL,
                      PHONE_EXTN = FOLDER_REC.PRIMARY_EXTN,
                      PREFERRED_NAME = FOLDER_REC.PREFERRED_NAME,
                      SECOND_NAME = FOLDER_REC.SECOND_NAME,
                      START_DT = FOLDER_REC.FIRST_COMMENCE,
                      SURNAME = FOLDER_REC.SURNAME,
                      THIRD_NAME = FOLDER_REC.THIRD_NAME,
                      TITLE = FOLDER_REC.TITLE,
                      USERNAME = FOLDER_REC.USERNAME
                WHERE EMPLOYEE# = FOLDER_REC.EMPLOYEE#;

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, 'UPDATE', FOLDER_REC.EMPLOYEE#);
            EXCEPTION
               -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'RECORD UPDATE WHEN OTHERS EXCEPTION', SQLERRM);
            END;
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      COMMIT;
   EXCEPTION
      -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'WHEN OTHERS EXCEPTION', SQLERRM);
   END TRIM_PAST_STAFF;


   PROCEDURE TRIM_STAFF_FOLDER_IU
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR FOLDER_CURSOR
      IS
         SELECT EMPLOYEE.EMPLOYEE#, OCCUP_TERM_DATE
           FROM EMPLOYEE, V_TRIM_STAFF_DETAILS
          WHERE EMPLOYEE.EMPLOYEE# = V_TRIM_STAFF_DETAILS.EMPLOYEE#;

      VAR_EMPLOYEE#          VARCHAR2(4000);
      VAR_END_DATE           VARCHAR2(4000);
      VAR_EXIST              VARCHAR2(4000);
      VAR_STATUS             VARCHAR2(4000);
      VAR_EMP_STATUS         VARCHAR2(4000);
      VAR_CLASSFN            VARCHAR2(4000);
      VAR_CREATED_DT         DATE;
      VAR_MODIFIED_DT        DATE;
      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'TRIM_STAFF_FOLDER_IU';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'TRIM_STAFF_FOLDER';
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR FOLDER_REC IN FOLDER_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_EXIST := NULL;
         VAR_STATUS := NULL;

         -- Check the employee status in HR
         BEGIN
            SELECT CREATED_DT, MODIFIED_DT
              INTO VAR_CREATED_DT, VAR_MODIFIED_DT
              FROM TRIM_STAFF_FOLDER
             WHERE EMPLOYEE# = FOLDER_REC.EMPLOYEE#;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               VAR_STATUS := 'CREATE';
         END;

         VAR_EMP_STATUS := NULL;

         -- Get the highest EMP_STATUS
         BEGIN
            SELECT EMP_STATUS
              INTO VAR_EMP_STATUS
              FROM (SELECT EMPLOYEE#, EMP_STATUS,
                           DECODE(EMP_STATUS,  'OFT', 1,  'OPT', 2,  'FFT', 3,  'FPT', 4,  'CASG', 5,  'CASA', 6,  'VISIT', 7,  8) AS NUM
                      FROM SUBSTANTIVE
                     WHERE EMPLOYEE# = FOLDER_REC.EMPLOYEE#
                    UNION
                    SELECT EMPLOYEE#, EMP_STATUS,
                           DECODE(EMP_STATUS,  'OFT', 1,  'OPT', 2,  'FFT', 3,  'FPT', 4,  'CASG', 5,  'CASA', 6,  'VISIT', 7,  8) AS NUM
                      FROM CONCURRENT
                     WHERE EMPLOYEE# = FOLDER_REC.EMPLOYEE#
                    ORDER BY NUM)
             WHERE ROWNUM = 1;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               VAR_EMP_STATUS := NULL;
         END;

         -- Get the highest CLASSIFICATION
         BEGIN
            SELECT TRIM(AWARD || ' ' || CLASSIFICATION)
              INTO VAR_CLASSFN
              FROM (SELECT EMPLOYEE#, CLASSIFICATION, AWARD,
                           DECODE(AWARD,  'OVER', 1,  'SSGA', 2,  'ASA', 3) AS AWARD_NUM
                      FROM SUBSTANTIVE
                     WHERE EMPLOYEE# = FOLDER_REC.EMPLOYEE#
                           AND ((AWARD = 'OVER')
                                OR (AWARD = 'SSGA'
                                    AND (   CLASSIFICATION LIKE '04%'
                                         OR CLASSIFICATION LIKE '05%'
                                         OR CLASSIFICATION LIKE '06%'))
                                OR (AWARD = 'ASA'
                                    AND (CLASSIFICATION = 'LEVD'
                                         OR CLASSIFICATION = 'LEVE')))
                    UNION
                    SELECT EMPLOYEE#, CLASSIFICATION, AWARD,
                           DECODE(AWARD,  'OVER', 1,  'SSGA', 2,  'ASA', 3) AS AWARD_NUM
                      FROM CONCURRENT
                     WHERE EMPLOYEE# = FOLDER_REC.EMPLOYEE#
                           AND ((AWARD = 'OVER')
                                OR (AWARD = 'SSGA'
                                    AND (   CLASSIFICATION LIKE '04%'
                                         OR CLASSIFICATION LIKE '05%'
                                         OR CLASSIFICATION LIKE '06%'))
                                OR (AWARD = 'ASA'
                                    AND (CLASSIFICATION = 'LEVD'
                                         OR CLASSIFICATION = 'LEVE')))
                    ORDER BY AWARD_NUM, CLASSIFICATION DESC)
             WHERE ROWNUM = 1;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               VAR_CLASSFN := NULL;
         END;

         -- keep the modified day
         IF VAR_STATUS = NULL
         THEN
            BEGIN
               SELECT MODIFIED_DT
                 INTO VAR_MODIFIED_DT
                 FROM TRIM_STAFF_FOLDER
                WHERE EMPLOYEE# = FOLDER_REC.EMPLOYEE#
                      AND END_DT = FOLDER_REC.OCCUP_TERM_DATE
                      AND NVL(HIGHEST_EMP_STATUS, 'NULL') =
                             NVL(VAR_EMP_STATUS, 'NULL')
                      AND NVL(HIGHEST_CLASSIFICATION, 'NULL') =
                             NVL(VAR_CLASSFN, 'NULL');
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  VAR_STATUS := 'MODIFIED';
            END;
         END IF;

         IF VAR_STATUS = 'MODIFIED'
         THEN
            VAR_MODIFIED_DT := SYSDATE;
         END IF;

         ------------------------------------------
         -- Constellar Hub : Actions = Insert, Update or Delete
         ------------------------------------------
         -- Perform insert, upsert or delete functions
         IF VAR_STATUS = 'CREATE'
         THEN
            BEGIN
               INSERT INTO TRIM_STAFF_FOLDER(CANCEL_DT, CREATED_DT, EMPLOYEE#,
                              END_DT, HIGHEST_CLASSIFICATION,
                              HIGHEST_EMP_STATUS, MODIFIED_DT)
                    VALUES (NULL, SYSDATE, FOLDER_REC.EMPLOYEE#, FOLDER_REC.OCCUP_TERM_DATE,
                            VAR_CLASSFN, VAR_EMP_STATUS, VAR_MODIFIED_DT);

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, 'INSERT', FOLDER_REC.EMPLOYEE#);
            EXCEPTION
               -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'RECORD INSERT WHEN OTHERS EXCEPTION', SQLERRM);
            END;
         ELSE
            BEGIN
               UPDATE TRIM_STAFF_FOLDER
                  SET CANCEL_DT = NULL, CREATED_DT = VAR_CREATED_DT,
                      EMPLOYEE# = FOLDER_REC.EMPLOYEE#,
                      END_DT = FOLDER_REC.OCCUP_TERM_DATE,
                      HIGHEST_CLASSIFICATION = VAR_CLASSFN,
                      HIGHEST_EMP_STATUS = VAR_EMP_STATUS,
                      MODIFIED_DT = VAR_MODIFIED_DT
                WHERE EMPLOYEE# = FOLDER_REC.EMPLOYEE#;

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, 'UPDATE', FOLDER_REC.EMPLOYEE#);
            EXCEPTION
               -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'RECORD UPDATE WHEN OTHERS EXCEPTION', SQLERRM);
            END;
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      COMMIT;
   EXCEPTION
      -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'WHEN OTHERS EXCEPTION', SQLERRM);
   END TRIM_STAFF_FOLDER_IU;


   PROCEDURE POST_INTERFACE
   IS
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Post Interface Rule
      ------------------------------------------
      HUB_LIB.SET_RUN_DATES(GC_INTERFACE_NAME, G_RUN_START, G_RUN_END);
   EXCEPTION
      -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, 'POST_INTERFACE', 'ERROR', NULL, 'POST_INTERFACE WHEN OTHERS EXCEPTION', SQLERRM);
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
      HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, C_THIS_PROC, 'INFO', NULL, 'Starting ' || GC_INTERFACE_NAME, 'Start at: ' || TO_CHAR(L_START_TIME));

      BEGIN
         L_PHASE := 'EXECUTE PRE_INTERFACE';
         --PRE_INTERFACE;

         L_PHASE := 'EXECUTE TRANSACTIONS';
         -- TRIM_PAST_STAFF; -- Disabled in Transformation Manager.
         TRIM_STAFF_FOLDER_IU;

         L_PHASE := 'EXECUTE POST_INTERFACE';
      --POST_INTERFACE;
      EXCEPTION
         -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
         WHEN OTHERS
         THEN
            HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, C_THIS_PROC, 'ERROR', NULL, 'MAIN_CONTROL WHEN OTHERS EXCEPTION during Phase: ' || L_PHASE, SQLERRM);
      END;

      -- Log that this interface has finished.
      L_END_TIME := LOCALTIMESTAMP;
      L_ELAPSED_TIME := L_END_TIME - L_START_TIME;
      HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, C_THIS_PROC, 'INFO', NULL, 'Elapsed time ' || L_ELAPSED_TIME, 'Ended at: ' || TO_CHAR(L_END_TIME));
   END MAIN_CONTROL;
END TRIM_FOLDER;
/
