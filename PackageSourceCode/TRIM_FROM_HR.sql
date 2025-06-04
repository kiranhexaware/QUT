CREATE OR REPLACE PACKAGE TRIM_FROM_HR
AS
   PROCEDURE MAIN_CONTROL;
END TRIM_FROM_HR;
/


CREATE OR REPLACE PACKAGE BODY TRIM_FROM_HR
AS

-- Deployed on: Tue Oct 30 15:58:09 AEST 2018
-- Branch/rev:1.0.3/544288f2b056ba57ede3c5fe94d524596283551a
-- Repo:ssh://git@repo.qut.edu.au:7999/qh/trim_from_hr.git
-- Deployed from: intdeploy.qut.edu.au:/home/integsvc/novoP/trim_from_hr/TRIM_from_HR.pkb

   GC_INTERFACE_NAME   CONSTANT VARCHAR2(20) := 'TRIM_from_HR';
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

   PROCEDURE TRIM_JOB_IU
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR JOB_CUSOR
      IS
         SELECT * FROM QUT_TODAYCURRENTOCCUPANCYVIEW;

      VAR_SOURCE             VARCHAR2(4000);
      VAR_STATUS             VARCHAR2(4000);
      VAR_CREATED_DT         DATE;
      VAR_MODIFIED_DT        DATE;
      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'TRIM_JOB_IU';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'TRIM_JOB';
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR JOB_REC IN JOB_CUSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         IF JOB_REC.OCC = 'HDA'
         THEN
            VAR_SOURCE := 1;
         ELSIF JOB_REC.OCC = 'CON'
         THEN
            VAR_SOURCE := 2;
         ELSE
            VAR_SOURCE := 3;
         END IF;

         VAR_STATUS := NULL;

         -- Check if the job existed in trim
         BEGIN
            SELECT CREATED_DT, MODIFIED_DT
              INTO VAR_CREATED_DT, VAR_MODIFIED_DT
              FROM TRIM_JOB
             WHERE     EMPLOYEE# = JOB_REC.EMPLOYEE#
                   AND JOB# = JOB_REC.JOB#
                   AND POSITION# = JOB_REC.POSITION#;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               VAR_STATUS := 'CREATE';
         END;

         -- Get the modified date time value from trim
         IF VAR_STATUS IS NULL
         THEN
            BEGIN
               SELECT MODIFIED_DT
                 INTO VAR_MODIFIED_DT
                 FROM TRIM_JOB
                WHERE     EMPLOYEE# = JOB_REC.EMPLOYEE#
                      AND JOB# = JOB_REC.JOB#
                      AND POSITION# = JOB_REC.POSITION#
                      AND COMMENCE_DATE = JOB_REC.COMMENCE_DATE
                      AND OCCUP_TERM_DATE = JOB_REC.OCCUP_TERM_DATE
                      AND SOURCE = VAR_SOURCE
                      AND JOB_CLEVEL = JOB_REC.CLEVEL;
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
         -- VAR_STATUS is still NULL when the record is existing and not updated
         IF VAR_STATUS = 'CREATE'
         THEN
            BEGIN
               INSERT INTO TRIM_JOB(CANCEL_DT, COMMENCE_DATE, CREATED_DT,
                              EMPLOYEE#, JOB#, JOB_CLEVEL, MODIFIED_DT,
                              OCCUP_TERM_DATE, POSITION#, SOURCE)
                    VALUES (NULL, JOB_REC.COMMENCE_DATE, SYSDATE, JOB_REC.EMPLOYEE#,
                            JOB_REC.JOB#, JOB_REC.CLEVEL, VAR_MODIFIED_DT,
                            JOB_REC.OCCUP_TERM_DATE, JOB_REC.POSITION#,
                            VAR_SOURCE);

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, 'INSERT', JOB_REC.EMPLOYEE# || '|' || JOB_REC.JOB# || '|' || JOB_REC.POSITION#);
            EXCEPTION
               -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'RECORD INSERT WHEN OTHERS EXCEPTION', SQLERRM);
            END;
         ELSIF VAR_STATUS = 'MODIFIED'
         THEN
            BEGIN
               UPDATE TRIM_JOB
                  SET CANCEL_DT = NULL, COMMENCE_DATE = JOB_REC.COMMENCE_DATE,
                      CREATED_DT = VAR_CREATED_DT, EMPLOYEE# = JOB_REC.EMPLOYEE#,
                      JOB# = JOB_REC.JOB#, JOB_CLEVEL = JOB_REC.CLEVEL,
                      MODIFIED_DT = VAR_MODIFIED_DT,
                      OCCUP_TERM_DATE = JOB_REC.OCCUP_TERM_DATE,
                      POSITION# = JOB_REC.POSITION#, SOURCE = VAR_SOURCE
                WHERE     EMPLOYEE# = JOB_REC.EMPLOYEE#
                      AND POSITION# = JOB_REC.POSITION#
                      AND JOB# = JOB_REC.JOB#;

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, 'UPDATE', JOB_REC.EMPLOYEE# || '|' || JOB_REC.JOB# || '|' || JOB_REC.POSITION#);
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

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      BEGIN
         DECLARE
            CURSOR POST_CURSOR
            IS
               SELECT DISTINCT EMPLOYEE#, JOB#, POSITION#
                 FROM TRIM_JOB
                WHERE CANCEL_DT IS NULL
               MINUS
               SELECT DISTINCT EMPLOYEE#, JOB#, POSITION#
                 FROM QUT_TODAYCURRENTOCCUPANCYVIEW;
         BEGIN
            FOR ROW IN POST_CURSOR
            LOOP
               BEGIN
                  UPDATE TRIM_JOB TJ
                     SET TJ.CANCEL_DT = SYSDATE
                   WHERE     TJ.EMPLOYEE# = ROW.EMPLOYEE#
                         AND TJ.POSITION# = ROW.POSITION#
                         AND TJ.JOB# = ROW.JOB#;

                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, 'POST TRANSACTION UPDATE', ROW.EMPLOYEE# || '|' || ROW.JOB# || '|' || ROW.POSITION#);
               END;
            END LOOP;

            COMMIT;
         EXCEPTION
            -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
            WHEN OTHERS
            THEN
               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'POST TRANSACTION WHEN OTHERS EXCEPTION', SQLERRM);
         END;
      END;
   EXCEPTION
      -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'WHEN OTHERS EXCEPTION', SQLERRM);
   END TRIM_JOB_IU;

   PROCEDURE SET_TRIM_ORG_TREE_REC(P_ACTION IN VARCHAR2,
      P_TRANSACTION_NAME IN VARCHAR2, P_CANCEL_DT IN DATE, P_CODE IN VARCHAR2,
      P_CREATED_DT IN DATE, P_DESCRIPTION IN VARCHAR2, P_KIND IN VARCHAR2,
      P_MODIFIED_DT IN DATE, P_PARENT_CODE IN VARCHAR2,
      P_PARENT_DESC IN VARCHAR2)
   IS
      VAR_TABLE_NAME   VARCHAR2(4000) := 'TRIM_ORG_TREE';
   BEGIN
      IF P_ACTION = 'CREATE'
      THEN
         BEGIN
            INSERT INTO TRIM_ORG_TREE(CANCEL_DT, CODE, CREATED_DT,
                           DESCRIPTION, KIND, MODIFIED_DT, PARENT_CODE,
                           PARENT_DESC)
                 VALUES (P_CANCEL_DT, P_CODE, P_CREATED_DT, P_DESCRIPTION,
                         P_KIND, P_MODIFIED_DT, P_PARENT_CODE, P_PARENT_DESC);

            HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, P_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, 'INSERT', P_CODE);
         EXCEPTION
            -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
            WHEN OTHERS
            THEN
               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, P_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'RECORD INSERT WHEN OTHERS EXCEPTION', SQLERRM);
         END;
      ELSE
         BEGIN
            UPDATE TRIM_ORG_TREE
               SET CANCEL_DT = P_CANCEL_DT, CODE = P_CODE,
                   CREATED_DT = P_CREATED_DT, DESCRIPTION = P_DESCRIPTION,
                   KIND = P_KIND, MODIFIED_DT = P_MODIFIED_DT,
                   PARENT_CODE = P_PARENT_CODE, PARENT_DESC = P_PARENT_DESC
             WHERE CODE = P_CODE;

            HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, P_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, 'UPDATE', P_CODE);
         EXCEPTION
            -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
            WHEN OTHERS
            THEN
               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, P_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'RECORD UPDATE WHEN OTHERS EXCEPTION', SQLERRM);
         END;
      END IF;
   END SET_TRIM_ORG_TREE_REC;

   PROCEDURE TRIM_ORG_CLEVEL1_IU
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR CODES_CURSOR
      IS
         SELECT *
           FROM CODES
          WHERE CODES.KIND = 'CLEVEL1' AND CODES.CODE IN ('1');

      VAR_STATUS             VARCHAR2(4000);
      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'TRIM_ORG_CLEVEL1_IU';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'TRIM_ORG_TREE';
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR CODES_REC IN CODES_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_STATUS := NULL;

         -- Check if the job existed in trim
         BEGIN
            SELECT 'MODIFIED'
              INTO VAR_STATUS
              FROM TRIM_ORG_TREE
             WHERE CODE = CODES_REC.CODE;
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
            SET_TRIM_ORG_TREE_REC(VAR_STATUS, VAR_TRANSACTION_NAME, NULL, CODES_REC.CODE, NULL, CODES_REC.DESCRIPTION, CODES_REC.KIND, NULL, NULL, NULL);
         ELSE
            SET_TRIM_ORG_TREE_REC(VAR_STATUS, VAR_TRANSACTION_NAME, NULL, CODES_REC.CODE, NULL, CODES_REC.DESCRIPTION, CODES_REC.KIND, NULL, NULL, NULL);
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      COMMIT;
   EXCEPTION
      -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'WHEN OTHERS EXCEPTION', SQLERRM);
   END TRIM_ORG_CLEVEL1_IU;


   PROCEDURE TRIM_ORG_CLEVEL23_IU
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR CODES_CURSOR
      IS
         SELECT CODES.CODE, CODES.KIND, CODES.DESCRIPTION,
                PARENT.CODE AS PARENT_CODE,
                PARENT.DESCRIPTION AS PARENT_DESCRIPTION
           FROM CODES, CODES PARENT
          WHERE PARENT.CODE =
                   TO_CHAR(SUBSTR(CODES.CODE, 0, LENGTH(CODES.CODE) - 2))
                AND LENGTH(PARENT.KIND) = LENGTH(CODES.KIND)
                AND LENGTH(PARENT.KIND) = 7
                AND PARENT.KIND LIKE 'CLEVEL%'
                AND CODES.KIND LIKE 'CLEVEL%'
                AND SUBSTR(CODES.CODE, 0, 1) IN ('1')
                AND LENGTH(CODES.CODE) <= 5;

      VAR_STATUS             VARCHAR2(4000);
      VAR_EXIST              VARCHAR2(4000);
      VAR_CREATED_DT         DATE;
      VAR_MODIFIED_DT        DATE;
      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'TRIM_ORG_CLEVEL23_IU';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'TRIM_ORG_TREE';
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR CODES_REC IN CODES_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_STATUS := NULL;
         VAR_EXIST := NULL;

         -- Check if the job existed in trim
         BEGIN
            SELECT CREATED_DT, MODIFIED_DT
              INTO VAR_CREATED_DT, VAR_MODIFIED_DT
              FROM TRIM_ORG_TREE
             WHERE CODE = CODES_REC.CODE;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               VAR_STATUS := 'CREATE';
         END;

         IF VAR_STATUS IS NULL
         THEN
            BEGIN
               SELECT MODIFIED_DT
                 INTO VAR_MODIFIED_DT
                 FROM TRIM_ORG_TREE
                WHERE     KIND = CODES_REC.KIND
                      AND CODE = CODES_REC.CODE
                      AND DESCRIPTION = CODES_REC.DESCRIPTION
                      AND PARENT_CODE = CODES_REC.PARENT_CODE;
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
            SET_TRIM_ORG_TREE_REC(VAR_STATUS, VAR_TRANSACTION_NAME, NULL, CODES_REC.CODE, SYSDATE, CODES_REC.DESCRIPTION, CODES_REC.KIND, VAR_MODIFIED_DT, CODES_REC.PARENT_CODE, CODES_REC.PARENT_DESCRIPTION);
         ELSE
            SET_TRIM_ORG_TREE_REC(VAR_STATUS, VAR_TRANSACTION_NAME, NULL, CODES_REC.CODE, VAR_CREATED_DT, CODES_REC.DESCRIPTION, CODES_REC.KIND, VAR_MODIFIED_DT, CODES_REC.PARENT_CODE, CODES_REC.PARENT_DESCRIPTION);
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      COMMIT;
   EXCEPTION
      -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'WHEN OTHERS EXCEPTION', SQLERRM);
   END TRIM_ORG_CLEVEL23_IU;


   PROCEDURE TRIM_ORG_CLEVEL45_IU
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR CODES_CURSOR
      IS
         SELECT CODES.CODE, CODES.KIND, CODES.DESCRIPTION,
                PARENT.CODE AS PARENT_CODE,
                PARENT.DESCRIPTION AS PARENT_DESCRIPTION
           FROM CODES, CODES PARENT
          WHERE PARENT.CODE =
                   TO_CHAR(SUBSTR(CODES.CODE, 0, LENGTH(CODES.CODE) - 1))
                AND LENGTH(PARENT.KIND) = LENGTH(CODES.KIND)
                AND LENGTH(PARENT.KIND) = 7
                AND PARENT.KIND LIKE 'CLEVEL%'
                AND CODES.KIND LIKE 'CLEVEL%'
                AND SUBSTR(CODES.CODE, 0, 1) IN ('1')
                AND LENGTH(CODES.CODE) > 5;

      VAR_STATUS             VARCHAR2(4000);
      VAR_CREATED_DT         DATE;
      VAR_MODIFIED_DT        DATE;
      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'TRIM_ORG_CLEVEL45_IU';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'TRIM_ORG_TREE';
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR CODES_REC IN CODES_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_STATUS := NULL;

         -- Check if the job existed in trim
         BEGIN
            SELECT CREATED_DT, MODIFIED_DT
              INTO VAR_CREATED_DT, VAR_MODIFIED_DT
              FROM TRIM_ORG_TREE
             WHERE CODE = CODES_REC.CODE;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               VAR_STATUS := 'CREATE';
         END;

         IF VAR_STATUS IS NULL
         THEN
            BEGIN
               SELECT MODIFIED_DT
                 INTO VAR_MODIFIED_DT
                 FROM TRIM_ORG_TREE
                WHERE     KIND = CODES_REC.KIND
                      AND CODE = CODES_REC.CODE
                      AND DESCRIPTION = CODES_REC.DESCRIPTION
                      AND PARENT_CODE = CODES_REC.PARENT_CODE;
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
            SET_TRIM_ORG_TREE_REC(VAR_STATUS, VAR_TRANSACTION_NAME, NULL, CODES_REC.CODE, SYSDATE, CODES_REC.DESCRIPTION, CODES_REC.KIND, VAR_MODIFIED_DT, CODES_REC.PARENT_CODE, CODES_REC.PARENT_DESCRIPTION);
         ELSE
            SET_TRIM_ORG_TREE_REC(VAR_STATUS, VAR_TRANSACTION_NAME, NULL, CODES_REC.CODE, VAR_CREATED_DT, CODES_REC.DESCRIPTION, CODES_REC.KIND, VAR_MODIFIED_DT, CODES_REC.PARENT_CODE, CODES_REC.PARENT_DESCRIPTION);
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      BEGIN
         DECLARE
            CURSOR POST_CURSOR
            IS
               SELECT DISTINCT CODE
                 FROM TRIM_ORG_TREE
                WHERE CODE NOT IN ('1') AND CANCEL_DT IS NULL
               MINUS
               SELECT DISTINCT CODE
                 FROM CODES
                WHERE     CODES.KIND LIKE 'CLEVEL%'
                      AND LENGTH(CODES.KIND) = 7
                      AND SUBSTR(CODES.CODE, 0, 1) IN ('1')
                      AND LENGTH(CODES.CODE) > 1;
         BEGIN
            FOR ROW IN POST_CURSOR
            LOOP
               BEGIN
                  UPDATE TRIM_ORG_TREE T
                     SET T.CANCEL_DT = SYSDATE
                   WHERE T.CODE = ROW.CODE;

                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, 'POST TRANSACTION UPDATE', ROW.CODE);
               END;
            END LOOP;

            COMMIT;
         EXCEPTION
            -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
            WHEN OTHERS
            THEN
               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'POST TRANSACTION WHEN OTHERS EXCEPTION', SQLERRM);
         END;
      END;
   EXCEPTION
      -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'WHEN OTHERS EXCEPTION', SQLERRM);
   END TRIM_ORG_CLEVEL45_IU;

   PROCEDURE TRIM_PERSON_IU
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR PERSON_CUSOR
      IS
         SELECT EMPLOYEE.EMPLOYEE#, BARCODE, EMAIL_ALIAS, OCCUP_TERM_DATE,
                GENDER, INITIALS, PRIMARY_EXTN, NVL(EMPLOYEE.PREFERRED_NAME, EMPLOYEE.FIRST_NAME) AS PREFERRED_NAME,
                FIRST_COMMENCE, EMPLOYEE.SURNAME, EMPLOYEE.TITLE, USERNAME,
                DATE_OF_BIRTH, FIRST_NAME, SECOND_NAME, THIRD_NAME
           FROM EMPLOYEE, V_TRIM_PERSON_DETAILS
          WHERE EMPLOYEE.EMPLOYEE# = V_TRIM_PERSON_DETAILS.EMPLOYEE#;

      VAR_STATUS             VARCHAR2(4000);
      VAR_CREATED_DT         DATE;
      VAR_MODIFIED_DT        DATE;
      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'TRIM_PERSON_IU';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'TRIM_PERSON';
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR PERSON_REC IN PERSON_CUSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_STATUS := NULL;

         -- Check if the object existed in trim
         BEGIN
            SELECT CREATED_DT, MODIFIED_DT
              INTO VAR_CREATED_DT, VAR_MODIFIED_DT
              FROM TRIM_PERSON
             WHERE EMPLOYEE# = PERSON_REC.EMPLOYEE#;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               VAR_STATUS := 'CREATE';
         END;

         -- Get the modified date time value from trim
         IF VAR_STATUS IS NULL
         THEN
            BEGIN
               SELECT MODIFIED_DT
                 INTO VAR_MODIFIED_DT
                 FROM TRIM_PERSON
                WHERE     EMPLOYEE# = PERSON_REC.EMPLOYEE#
                      AND TITLE = PERSON_REC.TITLE
                      AND SURNAME = PERSON_REC.SURNAME
                      AND NVL(PREFERRED_NAME, 'NULL') =
                             NVL(PERSON_REC.PREFERRED_NAME, 'NULL')
                      AND NVL(EMAIL, 'NULL') =
                             NVL(PERSON_REC.EMAIL_ALIAS, 'NULL')
                      AND NVL(PHONE_EXTN, 'NULL') =
                             NVL(PERSON_REC.PRIMARY_EXTN, 'NULL')
                      AND NVL(USERNAME, 'NULL') =
                             NVL(PERSON_REC.USERNAME, 'NULL')
                      AND NVL(BARCODE, 'NULL') =
                             NVL(PERSON_REC.BARCODE, 'NULL')
                      AND GENDER = PERSON_REC.GENDER
                      AND NVL(INITIALS, 'NULL') =
                             NVL(PERSON_REC.INITIALS, 'NULL')
                      AND START_DT = PERSON_REC.FIRST_COMMENCE
                      AND END_DT = PERSON_REC.OCCUP_TERM_DATE
                      AND DATE_OF_BIRTH = PERSON_REC.DATE_OF_BIRTH
                      AND NVL(FIRST_NAME, 'NULL') =
                             NVL(PERSON_REC.FIRST_NAME, 'NULL')
                      AND NVL(SECOND_NAME, 'NULL') =
                             NVL(PERSON_REC.SECOND_NAME, 'NULL')
                      AND NVL(THIRD_NAME, 'NULL') =
                             NVL(PERSON_REC.THIRD_NAME, 'NULL');
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
               INSERT INTO TRIM_PERSON(BARCODE, CANCEL_DT, CREATED_DT, EMAIL,
                              EMPLOYEE#, END_DT, GENDER, INITIALS,
                              MODIFIED_DT, PHONE_EXTN, PREFERRED_NAME,
                              START_DT, SURNAME, TITLE, USERNAME,
                              DATE_OF_BIRTH, FIRST_NAME, SECOND_NAME,
                              THIRD_NAME)
                    VALUES (PERSON_REC.BARCODE, NULL, SYSDATE, PERSON_REC.EMAIL_ALIAS,
                            PERSON_REC.EMPLOYEE#, PERSON_REC.OCCUP_TERM_DATE,
                            PERSON_REC.GENDER, PERSON_REC.INITIALS,
                            VAR_MODIFIED_DT, PERSON_REC.PRIMARY_EXTN,
                            PERSON_REC.PREFERRED_NAME, PERSON_REC.FIRST_COMMENCE,
                            PERSON_REC.SURNAME, PERSON_REC.TITLE, PERSON_REC.USERNAME,
                            PERSON_REC.DATE_OF_BIRTH, PERSON_REC.FIRST_NAME,
                            PERSON_REC.SECOND_NAME, PERSON_REC.THIRD_NAME);

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, 'INSERT', PERSON_REC.EMPLOYEE#);
            EXCEPTION
               -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'RECORD INSERT WHEN OTHERS EXCEPTION', SQLERRM);
            END;
         ELSE
            BEGIN
               UPDATE TRIM_PERSON
                  SET BARCODE = PERSON_REC.BARCODE, CANCEL_DT = NULL,
                      CREATED_DT = VAR_CREATED_DT,
                      EMAIL = PERSON_REC.EMAIL_ALIAS,
                      EMPLOYEE# = PERSON_REC.EMPLOYEE#,
                      END_DT = PERSON_REC.OCCUP_TERM_DATE,
                      GENDER = PERSON_REC.GENDER,
                      INITIALS = PERSON_REC.INITIALS,
                      MODIFIED_DT = VAR_MODIFIED_DT,
                      PHONE_EXTN = PERSON_REC.PRIMARY_EXTN,
                      PREFERRED_NAME = PERSON_REC.PREFERRED_NAME,
                      START_DT = PERSON_REC.FIRST_COMMENCE,
                      SURNAME = PERSON_REC.SURNAME, TITLE = PERSON_REC.TITLE,
                      USERNAME = PERSON_REC.USERNAME,
                      DATE_OF_BIRTH = PERSON_REC.DATE_OF_BIRTH,
                      FIRST_NAME = PERSON_REC.FIRST_NAME,
                      SECOND_NAME = PERSON_REC.SECOND_NAME,
                      THIRD_NAME = PERSON_REC.THIRD_NAME
                WHERE EMPLOYEE# = PERSON_REC.EMPLOYEE#;

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, 'UPDATE', PERSON_REC.EMPLOYEE#);
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

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      BEGIN
         DECLARE
            CURSOR POST_CURSOR
            IS
               SELECT DISTINCT EMPLOYEE#
                 FROM TRIM_PERSON
                WHERE CANCEL_DT IS NULL
               MINUS
               SELECT DISTINCT E.EMPLOYEE#
                 FROM V_TRIM_PERSON_DETAILS V, EMPLOYEE E
                WHERE V.EMPLOYEE# = E.EMPLOYEE#
                      AND V.OCCUP_TERM_DATE >= TRUNC(SYSDATE);
         BEGIN
            FOR ROW IN POST_CURSOR
            LOOP
               BEGIN
                  UPDATE TRIM_PERSON TP
                     SET TP.CANCEL_DT = SYSDATE,
                         TP.END_DT =
                            (SELECT MAX(S.OCCUP_TERM_DATE)
                               FROM SUBSTANTIVE S
                              WHERE S.EMPLOYEE# = ROW.EMPLOYEE#)
                   WHERE TP.EMPLOYEE# = ROW.EMPLOYEE#;

                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, 'POST TRANSACTION UPDATE', ROW.EMPLOYEE#);
               END;
            END LOOP;

            COMMIT;
         EXCEPTION
            -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
            WHEN OTHERS
            THEN
               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'TRIM_PERSON_IU POST TRANSACTION WHEN OTHERS EXCEPTION', SQLERRM);
         END;
      END;
   EXCEPTION
      -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'TRIM_PERSON_IU WHEN OTHERS EXCEPTION', SQLERRM);
   END TRIM_PERSON_IU;


   PROCEDURE TRIM_POSITION_COM_TREE_IU
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR POSITION_CUSOR
      IS
         SELECT * FROM V_TRIM_QUT_POS_HIER_CURR;

      VAR_STATUS             VARCHAR2(4000);
      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'TRIM_POSITION_COM_TREE_IU';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'TRIM_POSITION_TREE';
      VAR_CREATED_DT         DATE;
      VAR_MODIFIED_DT        DATE;
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Transaction Actions
      ------------------------------------------
      HR_TRIM.BUILDPOSTREEFROMDEFAULT;

      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR POSITION_REC IN POSITION_CUSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_STATUS := NULL;

         -- Check if the object existed in trim
         BEGIN
            SELECT CREATED_DT, MODIFIED_DT
              INTO VAR_CREATED_DT, VAR_MODIFIED_DT
              FROM TRIM_POSITION_TREE
             WHERE POS_ID = POSITION_REC.POSITION#;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               VAR_STATUS := 'CREATE';
         END;

         -- Get the modified date time value from trim
         IF VAR_STATUS IS NULL
         THEN
            BEGIN
               SELECT MODIFIED_DT
                 INTO VAR_MODIFIED_DT
                 FROM TRIM_POSITION_TREE
                WHERE     POS_ID = POSITION_REC.POSITION#
                      AND POS_TITLE = POSITION_REC.POS_TITLE
                      AND CLEVEL = POSITION_REC.CLEVEL
                      AND NVL(MGR_ID, 'NULL') =
                             NVL(POSITION_REC.MANAGER_POS#, 'NULL')
                      AND EMP_STATUS = POSITION_REC.EMP_STATUS
                      AND START_DATE = POSITION_REC.START_DATE
                      AND END_DATE = POSITION_REC.END_DATE;
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
               INSERT INTO TRIM_POSITION_TREE(CANCEL_DT, CLEVEL, CREATED_DT,
                              DEPTH, EMP_STATUS, END_DATE, LINEAGE, MGR_ID,
                              MODIFIED_DT, POS_ID, POS_TITLE, START_DATE)
                    VALUES (NULL, POSITION_REC.CLEVEL, SYSDATE, POSITION_REC.HIERARCHY_LEVEL,
                            POSITION_REC.EMP_STATUS, POSITION_REC.END_DATE,
                            NULL, POSITION_REC.MANAGER_POS#, VAR_MODIFIED_DT,
                            POSITION_REC.POSITION#, POSITION_REC.POS_TITLE,
                            POSITION_REC.START_DATE);

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, 'INSERT', POSITION_REC.POSITION#);
            EXCEPTION
               -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, VAR_TRANSACTION_NAME || ' RECORD INSERT WHEN OTHERS EXCEPTION', SQLERRM);
            END;
         ELSE
            BEGIN
               UPDATE TRIM_POSITION_TREE
                  SET CANCEL_DT = NULL, CLEVEL = POSITION_REC.CLEVEL,
                      CREATED_DT = VAR_CREATED_DT,
                      DEPTH = POSITION_REC.HIERARCHY_LEVEL,
                      EMP_STATUS = POSITION_REC.EMP_STATUS,
                      END_DATE = POSITION_REC.END_DATE, LINEAGE = NULL,
                      MGR_ID = POSITION_REC.MANAGER_POS#,
                      MODIFIED_DT = VAR_MODIFIED_DT,
                      POS_ID = POSITION_REC.POSITION#,
                      POS_TITLE = POSITION_REC.POS_TITLE,
                      START_DATE = POSITION_REC.START_DATE
                WHERE POS_ID = POSITION_REC.POSITION#;

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, 'UPDATE', POSITION_REC.POSITION#);
            EXCEPTION
               -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, VAR_TRANSACTION_NAME || ' RECORD UPDATE WHEN OTHERS EXCEPTION', SQLERRM);
            END;
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      BEGIN
         DECLARE
            VAR_CREATED_DT   VARCHAR2(4000) := NULL;

            CURSOR POST_CURSOR
            IS
               SELECT DISTINCT POS.POS_ID
                 FROM TRIM_POSITION_TREE POS
                WHERE CANCEL_DT IS NULL
               MINUS
               SELECT DISTINCT QUT.POSITION#
                 FROM V_TRIM_QUT_POS_HIER_CURR QUT;
         BEGIN
            FOR ROW IN POST_CURSOR
            LOOP
               BEGIN
                  UPDATE TRIM_POSITION_TREE TP
                     SET TP.CANCEL_DT = SYSDATE,
                         TP.END_DATE =
                            (SELECT END_DATE
                               FROM POSITION POS
                              WHERE POS.START_DATE =
                                       (SELECT MAX(PC.START_DATE)
                                          FROM POSITION PC
                                         WHERE PC.POSITION# = POS.POSITION#)
                                    AND POS.POSITION# = ROW.POS_ID)
                   WHERE TP.POS_ID = ROW.POS_ID;

                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, 'POST TRANSACTION UPDATE', ROW.POS_ID);
               END;
            END LOOP;

            COMMIT;
         EXCEPTION
            -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
            WHEN OTHERS
            THEN
               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, VAR_TRANSACTION_NAME || ' POST TRANSACTION WHEN OTHERS EXCEPTION', SQLERRM);
         END;
      END;
   EXCEPTION
      -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, VAR_TRANSACTION_NAME, VAR_TABLE_NAME, VAR_TRANSACTION_NAME || ' WHEN OTHERS EXCEPTION', SQLERRM);
   END TRIM_POSITION_COM_TREE_IU;

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

         TRIM_ORG_CLEVEL1_IU;
         TRIM_ORG_CLEVEL23_IU;
         TRIM_ORG_CLEVEL45_IU;
         TRIM_POSITION_COM_TREE_IU;
         TRIM_JOB_IU;
         TRIM_PERSON_IU;

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
END TRIM_FROM_HR;
/
