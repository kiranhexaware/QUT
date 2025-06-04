CREATE OR REPLACE PACKAGE HR_QV
AS
   PROCEDURE MAIN_CONTROL;

   PROCEDURE MAIN_CONTROL2;
END HR_QV;
/


CREATE OR REPLACE PACKAGE BODY HR_QV
AS
   GC_INTERFACE_NAME   CONSTANT VARCHAR2(20) := 'HR_QV';
   G_RUN_START                  TIMESTAMP;
   G_RUN_END                    TIMESTAMP;
   
   G_RUN_START2                 TIMESTAMP;
   G_RUN_END2                   TIMESTAMP;
   G_MOVE_DATE_WINDOW_2         BOOLEAN;
   

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


   PROCEDURE PRE_INTERFACE
   IS
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Interface Rule
      ------------------------------------------
      --global_INTERFACE_NAME = NULL
      --global_RUNID          = NULL
      --global_JOBID          = NULL
      --global_RUN_START      = NULL
      --global_RUN_END        = NULL

      --HUB_LIB..GET_INTERFACE_DETAILS(global_INTERFACE_NAME, global_RUNID, global_JOBID)
      HUB_LIB.GET_RUN_DATES(GC_INTERFACE_NAME, G_RUN_START, G_RUN_END);
      OFFSET_RUN_TS(G_RUN_START);
   END PRE_INTERFACE;

   PROCEDURE EMP_ADDRESS
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR HR_CURSOR
      IS
         SELECT *
           FROM EMPLOYEE
          WHERE TP >= TRUNC(G_RUN_START)
                      - 1
                AND TP <= TRUNC(G_RUN_END);

      VAR_DESCRIPTION        VARCHAR2(4000);
      VAR_KEY_DATA           VARCHAR2(4000);
      VAR_STATUS             VARCHAR2(4000);
      VAR_CREATED_DT         DATE;
      VAR_MODIFIED_DT        DATE;
      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'EMP_ADDRESS';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'EMP_ADDRESS';
      VAR_ADDR_LINE1         VARCHAR2(4000);
      VAR_ADDR_LINE2         VARCHAR2(4000);
      VAR_ADDR_LINE3         VARCHAR2(4000);
      VAR_ADDR_LINE4         VARCHAR2(4000);
      VAR_ADDR_LINE5         VARCHAR2(4000);
      VAR_ADDR_TYPE          VARCHAR2(4000);
      VAR_EMPLOYEE_ID        VARCHAR2(4000);
      VAR_POST_CD            VARCHAR2(4000);
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Transaction Actions
      ------------------------------------------
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR HR_REC IN HR_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         FOR ITERATION IN 1 .. 2
         LOOP
            ------------------------------------------
            -- Constellar Hub : Each Record Actions
            ------------------------------------------
            -- Prepare value for each field in the records and set them to a local variable
            IF ITERATION = 1
            THEN
               VAR_ADDR_LINE1 := HR_REC.STREET_ADDRESS;
               VAR_ADDR_LINE2 := HR_REC.TOWN_ADDRESS;
               VAR_ADDR_LINE3 := HR_REC.ADDRESS3;
               VAR_ADDR_LINE4 := HR_REC.ADDRESS4;
               VAR_ADDR_LINE5 := HR_REC.STATE_CODE;
               VAR_ADDR_TYPE := 'HOME';
               VAR_POST_CD := HR_REC.POST_CODE;
            ELSE
               VAR_ADDR_LINE1 := HR_REC.STREET_ADDRESS_POST;
               VAR_ADDR_LINE2 := HR_REC.TOWN_ADDRESS_POST;
               VAR_ADDR_LINE3 := HR_REC.ADDRESS_POST3;
               VAR_ADDR_LINE4 := HR_REC.ADDRESS_POST4;
               VAR_ADDR_LINE5 := HR_REC.STATE_CODE_POST;
               VAR_ADDR_TYPE := 'MAILING';
               VAR_POST_CD := HR_REC.POST_CODE_POST;
            END IF;

            VAR_EMPLOYEE_ID := HR_REC.EMPLOYEE#;

            VAR_STATUS := NULL;
            VAR_KEY_DATA :=
                  'EMPLOYEE#: '
               || VAR_EMPLOYEE_ID
               || '|ADDR_TYPE: '
               || VAR_ADDR_TYPE;

            -- Check if the target record existed
            BEGIN
               SELECT 'UPDATE'
                 INTO VAR_STATUS
                 FROM EMP_ADDRESS
                WHERE EMPLOYEE_ID = VAR_EMPLOYEE_ID
                      AND ADDR_TYPE = VAR_ADDR_TYPE;
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
                  INSERT INTO EMP_ADDRESS(ADDR_LINE_1, ADDR_LINE_2,
                                 ADDR_LINE_3, ADDR_LINE_4, ADDR_LINE_5,
                                 ADDR_TYPE, EMPLOYEE_ID, POST_CD)
                       VALUES (VAR_ADDR_LINE1, VAR_ADDR_LINE2, VAR_ADDR_LINE3,
                               VAR_ADDR_LINE4, VAR_ADDR_LINE5, VAR_ADDR_TYPE,
                               VAR_EMPLOYEE_ID, VAR_POST_CD);

                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                       VAR_TRANSACTION_NAME,
                                       'ERROR',
                                       VAR_TABLE_NAME,
                                       VAR_KEY_DATA,
                                       'RECORD INSERT WHEN OTHERS EXCEPTION: '
                                       || SQLERRM);
                     RAISE;
               END;
            ELSE
               BEGIN
                  UPDATE EMP_ADDRESS
                     SET ADDR_LINE_1 = VAR_ADDR_LINE1,
                         ADDR_LINE_2 = VAR_ADDR_LINE2,
                         ADDR_LINE_3 = VAR_ADDR_LINE3,
                         ADDR_LINE_4 = VAR_ADDR_LINE4,
                         ADDR_LINE_5 = VAR_ADDR_LINE5,
                         ADDR_TYPE = VAR_ADDR_TYPE,
                         EMPLOYEE_ID = VAR_EMPLOYEE_ID, POST_CD = VAR_POST_CD
                   WHERE EMPLOYEE_ID = VAR_EMPLOYEE_ID
                         AND ADDR_TYPE = VAR_ADDR_TYPE;

                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                       VAR_TRANSACTION_NAME,
                                       'ERROR',
                                       VAR_TABLE_NAME,
                                       VAR_KEY_DATA,
                                       'RECORD UPDATE WHEN OTHERS EXCEPTION: '
                                       || SQLERRM);
                     RAISE;
               END;
            END IF;
         END LOOP;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      -- Unique-per-source
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                           VAR_TRANSACTION_NAME,
                           'ERROR',
                           VAR_TABLE_NAME,
                           VAR_KEY_DATA,
                           'WHEN OTHERS EXCEPTION: '
                           || SQLERRM);
         ROLLBACK;
   END EMP_ADDRESS;

   PROCEDURE EMP_COUNTRY
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR HR_CURSOR
      IS
         SELECT *
           FROM CODES
          WHERE KIND = 'COUNTRY';

      VAR_KEY_DATA           VARCHAR2(4000);
      VAR_STATUS             VARCHAR2(4000);
      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'EMP_COUNTRY';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'EMP_COUNTRY_CD';
      VAR_COUNTRY_CD         VARCHAR2(4000);
      VAR_DESCRIPTION        VARCHAR2(4000);
      VAR_ACTIVE_IND         VARCHAR2(4000);
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Transaction Actions
      ------------------------------------------
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR HR_REC IN HR_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_ACTIVE_IND := 'Y';
         VAR_COUNTRY_CD := HR_REC.CODE;
         VAR_DESCRIPTION := HR_REC.DESCRIPTION;

         VAR_STATUS := NULL;
         VAR_KEY_DATA :=
            'COUNTRY_CD: '
            || VAR_COUNTRY_CD;

         BEGIN
            SELECT 'UPDATE'                
              INTO VAR_STATUS
              FROM EMP_COUNTRY_CD
             WHERE COUNTRY_CD = VAR_COUNTRY_CD;
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
               INSERT
                 INTO EMP_COUNTRY_CD(ACTIVE_IND, COUNTRY_CD, DESCRIPTION)
               VALUES (VAR_ACTIVE_IND, VAR_COUNTRY_CD, VAR_DESCRIPTION);

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD INSERT WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         ELSIF VAR_STATUS = 'UPDATE'
         THEN
            BEGIN
               UPDATE EMP_COUNTRY_CD
                  SET ACTIVE_IND = VAR_ACTIVE_IND,
                      DESCRIPTION = VAR_DESCRIPTION,
                      COUNTRY_CD = VAR_COUNTRY_CD
                WHERE COUNTRY_CD = VAR_COUNTRY_CD;

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD UPDATE WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      -- Unique-per-source
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                           VAR_TRANSACTION_NAME,
                           'ERROR',
                           VAR_TABLE_NAME,
                           VAR_KEY_DATA,
                           'WHEN OTHERS EXCEPTION: '
                           || SQLERRM);
         ROLLBACK;
   END EMP_COUNTRY;


   PROCEDURE EMP_LANGUAGE
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR HR_CURSOR
      IS
         SELECT *
           FROM CODES
          WHERE KIND IN ('LANGUAGE_HOME');

      VAR_KEY_DATA           VARCHAR2(4000);
      VAR_STATUS             VARCHAR2(4000);
      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'EMP_LANGUAGE';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'EMP_LANGUAGE_CD';
      VAR_LANGUAGE_CD        VARCHAR2(4000);
      VAR_DESCRIPTION        VARCHAR2(4000);
      VAR_ACTIVE_IND         VARCHAR2(4000);
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Transaction Actions
      ------------------------------------------
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR HR_REC IN HR_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_ACTIVE_IND := 'Y';
         VAR_LANGUAGE_CD := HR_REC.CODE;
         VAR_DESCRIPTION := HR_REC.DESCRIPTION;

         VAR_STATUS := NULL;
         VAR_KEY_DATA :=
            'LANGUAGE_CD: '
            || VAR_LANGUAGE_CD;

         BEGIN
            SELECT 'UPDATE'                
              INTO VAR_STATUS
              FROM EMP_LANGUAGE_CD
             WHERE LANGUAGE_CD = VAR_LANGUAGE_CD;
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
               INSERT INTO EMP_LANGUAGE_CD(ACTIVE_IND, LANGUAGE_CD,
                              DESCRIPTION)
                    VALUES (VAR_ACTIVE_IND, VAR_LANGUAGE_CD, VAR_DESCRIPTION);

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD INSERT WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         -- According to the logic in CH, it will never do the update because the record will be dropped if there is equal or more than 1 record(s) found in the table
         --
         --         ELSIF VAR_STATUS = 'UPDATE'
         --         THEN
         --            BEGIN
         --               UPDATE EMP_LANGUAGE_CD
         --                  SET ACTIVE_IND = VAR_ACTIVE_IND,
         --                      DESCRIPTION = VAR_DESCRIPTION,
         --                      LANGUAGE_CD = VAR_LANGUAGE_CD
         --                WHERE LANGUAGE_CD = VAR_LANGUAGE_CD;
         --
         --               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
         --            EXCEPTION
         --               WHEN OTHERS
         --               THEN
         --                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
         --                                    VAR_TRANSACTION_NAME,
         --                                    'ERROR',
         --                                    VAR_TABLE_NAME,
         --                                    VAR_KEY_DATA,
         --                                    'RECORD UPDATE WHEN OTHERS EXCEPTION: '
         --                                    || SQLERRM);
         --                  RAISE;
         --            END;
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      -- Unique-per-source
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                           VAR_TRANSACTION_NAME,
                           'ERROR',
                           VAR_TABLE_NAME,
                           VAR_KEY_DATA,
                           'WHEN OTHERS EXCEPTION: '
                           || SQLERRM);
         ROLLBACK;
   END EMP_LANGUAGE;

   PROCEDURE EMP_EMPLOYEE
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR HR_CURSOR
      IS
         SELECT *
           FROM EMPLOYEE
          WHERE TP >= TRUNC(G_RUN_START)
                      - 1
                AND TP <= TRUNC(G_RUN_END);

      VAR_KEY_DATA           VARCHAR2(4000);
      VAR_STATUS             VARCHAR2(4000);
      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'EMP_EMPLOYEE';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'EMP_EMPLOYEE';
      VAR_BIRTH_DT           date;
      VAR_COMMENCEMENT_DT    DATE;
      VAR_EMPLOYEE_ID        VARCHAR2(4000);
      VAR_FIRST_NAME         VARCHAR2(4000);
      VAR_GENDER             VARCHAR2(4000);
      VAR_PREFERRED_NAME     VARCHAR2(4000);
      VAR_SECOND_NAME        VARCHAR2(4000);
      VAR_SURNAME            VARCHAR2(4000);
      VAR_THIRD_NAME         VARCHAR2(4000);
      VAR_TITLE              VARCHAR2(4000);
      
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Transaction Actions
      ------------------------------------------
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR HR_REC IN HR_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_BIRTH_DT := HR_REC.DATE_OF_BIRTH;
         VAR_COMMENCEMENT_DT := HR_REC.FIRST_COMMENCE;
         VAR_EMPLOYEE_ID := HR_REC.EMPLOYEE#;
         VAR_FIRST_NAME := HR_REC.FIRST_NAME;
         VAR_GENDER := HR_REC.GENDER;
         VAR_PREFERRED_NAME := HR_REC.PREFERRED_NAME;
         VAR_SECOND_NAME := HR_REC.SECOND_NAME;
         VAR_SURNAME := HR_REC.SURNAME;
         VAR_THIRD_NAME := HR_REC.THIRD_NAME;
         VAR_TITLE := HR_REC.TITLE;

         VAR_STATUS := NULL;
         VAR_KEY_DATA :=
            'EMPLOYEE_ID: '
            || VAR_EMPLOYEE_ID;

         BEGIN
            SELECT 'UPDATE'                
              INTO VAR_STATUS
              FROM EMP_EMPLOYEE
             WHERE EMPLOYEE_ID = VAR_EMPLOYEE_ID;
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
               INSERT INTO EMP_EMPLOYEE(BIRTH_DT, COMMENCEMENT_DT,
                              EMPLOYEE_ID, FIRST_NAME, GENDER, PREFERRED_NAME,
                              SECOND_NAME, SURNAME, THIRD_NAME, TITLE)
                    VALUES (VAR_BIRTH_DT, VAR_COMMENCEMENT_DT,
                            VAR_EMPLOYEE_ID, VAR_FIRST_NAME, VAR_GENDER,
                            VAR_PREFERRED_NAME, VAR_SECOND_NAME, VAR_SURNAME,
                            VAR_THIRD_NAME, VAR_TITLE);

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD INSERT WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         ELSIF VAR_STATUS = 'UPDATE'
         THEN
            BEGIN
               UPDATE EMP_EMPLOYEE
                  SET BIRTH_DT = VAR_BIRTH_DT,
                      COMMENCEMENT_DT = VAR_COMMENCEMENT_DT,
                      EMPLOYEE_ID = VAR_EMPLOYEE_ID,
                      FIRST_NAME = VAR_FIRST_NAME, GENDER = VAR_GENDER,
                      PREFERRED_NAME = VAR_PREFERRED_NAME,
                      SECOND_NAME = VAR_SECOND_NAME, SURNAME = VAR_SURNAME,
                      THIRD_NAME = VAR_THIRD_NAME, TITLE = VAR_TITLE
                WHERE EMPLOYEE_ID = VAR_EMPLOYEE_ID;

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD UPDATE WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         END IF;
         
         

      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      -- Unique-per-source
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                           VAR_TRANSACTION_NAME,
                           'ERROR',
                           VAR_TABLE_NAME,
                           VAR_KEY_DATA,
                           'WHEN OTHERS EXCEPTION: '
                           || SQLERRM);
         ROLLBACK;
   END EMP_EMPLOYEE;

   PROCEDURE EMP_POSITION
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR HR_CURSOR
      IS
         SELECT *
           FROM POSITION
          WHERE TP >= TRUNC(G_RUN_START)
                      - 1
                AND TP <= TRUNC(G_RUN_END);

      VAR_KEY_DATA                   VARCHAR2(4000);
      VAR_STATUS                     VARCHAR2(4000);
      VAR_TRANSACTION_NAME           VARCHAR2(4000) := 'EMP_POSITION';
      VAR_TABLE_NAME                 VARCHAR2(4000) := 'EMP_POSITION';
      VAR_CATEGORY_CD                VARCHAR2(4000);
      VAR_END_DT                     DATE;
      VAR_PARENT_POSITION_ID         VARCHAR2(4000);
      VAR_MAN_POSITION               VARCHAR2(4000);
      VAR_START_DATE                 DATE;
      VAR_DATE                       DATE;
      VAR_PARENT_POSITION_START_DT   DATE;
      VAR_POSITION_ID                VARCHAR2(4000);
      VAR_POSITION_TITLE             VARCHAR2(4000);
      VAR_START_DT                   DATE;
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Transaction Actions
      ------------------------------------------
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR HR_REC IN HR_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_CATEGORY_CD := HR_REC.POS_CATEGORY;
         VAR_END_DT := HR_REC.END_DATE;
         VAR_PARENT_POSITION_ID := HR_REC.MANAGER_POS#;

         VAR_MAN_POSITION := HR_REC.MANAGER_POS#;
         VAR_START_DATE := HR_REC.START_DATE;
         VAR_DATE := NULL;

         BEGIN
            SELECT MAX(START_DATE)
              INTO VAR_DATE
              FROM POSITION
             WHERE POSITION# = VAR_MAN_POSITION
                   AND START_DATE <= VAR_START_DATE;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               NULL;
         END;

         VAR_PARENT_POSITION_START_DT := VAR_DATE;
         VAR_POSITION_ID := HR_REC.POSITION#;
         VAR_POSITION_TITLE := HR_REC.POS_TITLE;
         VAR_START_DT := HR_REC.START_DATE;

         VAR_STATUS := NULL;
         VAR_KEY_DATA :=
               'POSITION_ID: '
            || VAR_POSITION_ID
            || '|START_DT: '
            || VAR_START_DT;

         BEGIN
            SELECT 'UPDATE'                
              INTO VAR_STATUS
              FROM EMP_POSITION
             WHERE POSITION_ID = VAR_POSITION_ID AND START_DT = VAR_START_DT;
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
               INSERT INTO EMP_POSITION(CATEGORY_CD, END_DT,
                              PARENT_POSITION_ID, PARENT_POSITION_START_DT,
                              POSITION_ID, POSITION_TITLE, START_DT)
                    VALUES (VAR_CATEGORY_CD, VAR_END_DT,
                            VAR_PARENT_POSITION_ID,
                            VAR_PARENT_POSITION_START_DT, VAR_POSITION_ID,
                            VAR_POSITION_TITLE, VAR_START_DT);

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD INSERT WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         ELSIF VAR_STATUS = 'UPDATE'
         THEN
            BEGIN
               UPDATE EMP_POSITION
                  SET CATEGORY_CD = VAR_CATEGORY_CD, END_DT = VAR_END_DT,
                      PARENT_POSITION_ID = VAR_PARENT_POSITION_ID,
                      PARENT_POSITION_START_DT = VAR_PARENT_POSITION_START_DT,
                      POSITION_ID = VAR_POSITION_ID,
                      POSITION_TITLE = VAR_POSITION_TITLE,
                      START_DT = VAR_START_DT
                WHERE POSITION_ID = VAR_POSITION_ID
                      AND START_DT = VAR_START_DT;

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD UPDATE WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      -- Unique-per-source
      UPDATE EMP_POSITION
         SET PARENT_POSITION_ID = NULL, PARENT_POSITION_START_DT = NULL
       WHERE PARENT_POSITION_ID = 'DEFAULT';

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                           VAR_TRANSACTION_NAME,
                           'ERROR',
                           VAR_TABLE_NAME,
                           VAR_KEY_DATA,
                           'WHEN OTHERS EXCEPTION: '
                           || SQLERRM);
         ROLLBACK;
   END EMP_POSITION;

   PROCEDURE EMP_EQUITY
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR HR_CURSOR
      IS
         SELECT *
           FROM EMPLOYEE_EQUITY
          WHERE     TP >= TRUNC(G_RUN_START)
                          - 1
                AND TP <= TRUNC(G_RUN_END);

      VAR_KEY_DATA           VARCHAR2(4000);
      VAR_STATUS             VARCHAR2(4000);
      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'EMP_EQUITY';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'EMP_EMPLOYEE_EQUITY';
      VAR_ABORIGINALITY      VARCHAR2(4000);
      VAR_COUNTRY_ORIGIN     VARCHAR2(4000);
      VAR_CULT_BACKGROUND    VARCHAR2(4000);
      VAR_DATE_ARRIVED       VARCHAR2(4000);
      VAR_DISABILITY         VARCHAR2(4000);
      VAR_DISAB_ASSIST       VARCHAR2(4000);
      VAR_EMPLOYEE_ID        VARCHAR2(4000);
      VAR_ETHNICITY          VARCHAR2(4000);
      VAR_LANGUAGE_HOME      VARCHAR2(4000);
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Transaction Actions
      ------------------------------------------
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR HR_REC IN HR_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_ABORIGINALITY := HR_REC.ABORIGINALITY;
         VAR_COUNTRY_ORIGIN := HR_REC.COUNTRY_ORIGIN;
         VAR_CULT_BACKGROUND := HR_REC.CULT_BACKGROUND;
         VAR_DATE_ARRIVED := HR_REC.DATE_ARRIVED;
         VAR_DISABILITY := HR_REC.DISABILITY;
         VAR_DISAB_ASSIST := HR_REC.DISAB_ASSIST;
         VAR_EMPLOYEE_ID := HR_REC.EMPLOYEE#;
         VAR_ETHNICITY := HR_REC.ETHNICITY;
         VAR_LANGUAGE_HOME := HR_REC.LANGUAGE_HOME;

         VAR_STATUS := NULL;
         VAR_KEY_DATA :=
            'EMPLOYEE_ID: '
            || VAR_EMPLOYEE_ID;

         BEGIN
            SELECT 'UPDATE'                
              INTO VAR_STATUS
              FROM EMP_EMPLOYEE_EQUITY
             WHERE EMPLOYEE_ID = VAR_EMPLOYEE_ID;
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
               INSERT INTO EMP_EMPLOYEE_EQUITY(ABORIGINALITY, COUNTRY_ORIGIN,
                              CULT_BACKGROUND, DATE_ARRIVED, DISABILITY,
                              DISAB_ASSIST, EMPLOYEE_ID, ETHNICITY,
                              LANGUAGE_HOME)
                    VALUES (VAR_ABORIGINALITY, VAR_COUNTRY_ORIGIN,
                            VAR_CULT_BACKGROUND, VAR_DATE_ARRIVED,
                            VAR_DISABILITY, VAR_DISAB_ASSIST, VAR_EMPLOYEE_ID,
                            VAR_ETHNICITY, VAR_LANGUAGE_HOME);

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD INSERT WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         ELSIF VAR_STATUS = 'UPDATE'
         THEN
            BEGIN
               UPDATE EMP_EMPLOYEE_EQUITY
                  SET ABORIGINALITY = VAR_ABORIGINALITY,
                      COUNTRY_ORIGIN = VAR_COUNTRY_ORIGIN,
                      CULT_BACKGROUND = VAR_CULT_BACKGROUND,
                      DATE_ARRIVED = VAR_DATE_ARRIVED,
                      DISABILITY = VAR_DISABILITY,
                      DISAB_ASSIST = VAR_DISAB_ASSIST,
                      EMPLOYEE_ID = VAR_EMPLOYEE_ID,
                      ETHNICITY = VAR_ETHNICITY,
                      LANGUAGE_HOME = VAR_LANGUAGE_HOME
                WHERE EMPLOYEE_ID = VAR_EMPLOYEE_ID;

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD UPDATE WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      -- Unique-per-source
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                           VAR_TRANSACTION_NAME,
                           'ERROR',
                           VAR_TABLE_NAME,
                           VAR_KEY_DATA,
                           'WHEN OTHERS EXCEPTION: '
                           || SQLERRM);
         ROLLBACK;
   END EMP_EQUITY;

   PROCEDURE EMP_SUBSTANTIVE
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR HR_CURSOR
      IS
         SELECT *
           FROM SUBSTANTIVE
          WHERE TP >= TRUNC(G_RUN_START)
                      - 1
                AND TP <= TRUNC(G_RUN_END)
                AND CLASSIFICATION NOT IN ('PRACT', 'SOC');

      l_discard_rec            BOOLEAN := FALSE;
      VAR_DESCRIPTION          VARCHAR2(4000);
      VAR_KEY_DATA             VARCHAR2(4000);
      VAR_STATUS               VARCHAR2(4000);
      VAR_TRANSACTION_NAME     VARCHAR2(4000) := 'EMP_SUBSTANTIVE';
      VAR_TABLE_NAME           VARCHAR2(4000) := 'EMP_EMPLOYEE_JOB_WK';
      VAR_POSITION             VARCHAR2(4000);
      VAR_AWARD                VARCHAR2(4000);
      VAR_CLEVEL               VARCHAR2(4000);
      VAR_ORG_UNIT             VARCHAR2(4000);
      VAR_CATEGORY_CD          VARCHAR2(4000);
      VAR_ORG_UNIT_CD          VARCHAR2(4000);
      VAR_CLASSIFICATION       VARCHAR2(4000);
      VAR_CLIENT_CATEGORY      VARCHAR2(4000);
      VAR_EMPLOYEE_ID          VARCHAR2(4000);
      VAR_END_DT               DATE;
      VAR_FRACTION             VARCHAR2(4000);
      VAR_JOB_ID               VARCHAR2(4000);
      VAR_JOB_TYPE             VARCHAR2(4000);
      VAR_POSITION_ID          VARCHAR2(4000);
      VAR_POSITION_INCREMENT   VARCHAR2(4000);
      VAR_COMMENCE             VARCHAR2(4000);
      VAR_START                DATE;
      VAR_ERROR                VARCHAR2(4000);
      VAR_POSITION_START_DT    DATE;
      VAR_POSITION_TITLE       VARCHAR2(4000);
      VAR_START_DT             DATE;
      VAR_STATUS_CD            VARCHAR2(4000);
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Transaction Actions
      ------------------------------------------
      FOR HR_REC IN HR_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : Pre Record Actions
         ------------------------------------------
         VAR_CLEVEL := HR_REC.CLEVEL;
         VAR_ORG_UNIT := NULL;

         BEGIN
            SELECT ORG_UNIT_CD
              INTO VAR_ORG_UNIT
              FROM EMP_ORG_UNIT
             WHERE ORG_UNIT_CD = VAR_CLEVEL AND END_DT IS NULL;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               VAR_ORG_UNIT := VAR_CLEVEL;
         END;

         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_AWARD := HR_REC.AWARD;

         IF VAR_AWARD IS NULL
         THEN
            VAR_AWARD := 'UNKWN';
         END IF;

         VAR_CATEGORY_CD := HR_REC.OCCUP_POS_CAT;
         VAR_CLASSIFICATION := HR_REC.CLASSIFICATION;
         VAR_CLIENT_CATEGORY := HR_REC.INOPERATIVE;
         VAR_EMPLOYEE_ID := HR_REC.EMPLOYEE#;
         VAR_END_DT := HRM_QUT_LIB.EXT_SUBTERM(HR_REC.EMPLOYEE#, HR_REC.JOB#, HR_REC.COMMENCE_DATE);
         VAR_FRACTION := HR_REC.POS_FRACTION;
         VAR_JOB_ID := HR_REC.JOB#;
         VAR_JOB_TYPE := 'SUB';
         VAR_ORG_UNIT_CD := VAR_ORG_UNIT;
         VAR_POSITION_ID := HR_REC.POSITION#;
         VAR_POSITION_INCREMENT := HR_REC.STEP#;

         VAR_POSITION := HR_REC.POSITION#;
         VAR_COMMENCE := HR_REC.COMMENCE_DATE;
         VAR_START := NULL;
         VAR_ERROR := NULL;
         VAR_STATUS := NULL;

         BEGIN
            SELECT NVL((SELECT MAX(START_DT)
                          FROM EMP_POSITION
                         WHERE POSITION_ID = VAR_POSITION
                               AND START_DT <= VAR_COMMENCE),
                       (SELECT MIN(START_DT)
                          FROM EMP_POSITION
                         WHERE POSITION_ID = VAR_POSITION))
                      AS START_DATE
              INTO VAR_START
              FROM DUAL;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               l_discard_rec := TRUE;
               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                              VAR_TRANSACTION_NAME,
                              'ERROR',
                              VAR_TABLE_NAME,
                              VAR_KEY_DATA,
                              'RECORD DISCARD: Inconsistent Start Date for Position: '
                              || VAR_POSITION);
         END;

         VAR_POSITION_START_DT := VAR_START; --TO_DATE(var_start,"DD/MM/YYYY")
         VAR_POSITION_TITLE := HR_REC.OCCUP_POS_TITLE;
         VAR_START_DT := TRUNC(HR_REC.COMMENCE_DATE);
         VAR_STATUS_CD := HR_REC.EMP_STATUS;

         VAR_STATUS := NULL;
         VAR_KEY_DATA :=
               'EMPLOYEE_ID: '
            || VAR_EMPLOYEE_ID
            || '|JOB_ID: '
            || VAR_JOB_ID
            || '|START_DT: '
            || VAR_START_DT
            || '|POSITION_ID: '
            || VAR_POSITION_ID
            || '|JOB_TYPE: '
            || VAR_JOB_TYPE;

         -- Check if the target record existed
         IF NOT l_discard_rec
         THEN
            BEGIN
               SELECT 'UPDATE'
                 INTO VAR_STATUS
                 FROM EMP_EMPLOYEE_JOB_WK
                WHERE     EMPLOYEE_ID = VAR_EMPLOYEE_ID
                      AND JOB_ID = VAR_JOB_ID
                      AND START_DT = VAR_START_DT
                      AND POSITION_ID = VAR_POSITION_ID
                      AND JOB_TYPE = VAR_JOB_TYPE;
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
                  INSERT INTO EMP_EMPLOYEE_JOB_WK(AWARD, CATEGORY_CD,
                                 CLASSIFICATION, CLIENT_CATEGORY, EMPLOYEE_ID,
                                 END_DT, FRACTION, JOB_ID, JOB_TYPE,
                                 ORG_UNIT_CD, POSITION_ID, POSITION_INCREMENT,
                                 POSITION_START_DT, POSITION_TITLE, START_DT,
                                 STATUS_CD)
                       VALUES (VAR_AWARD, VAR_CATEGORY_CD, VAR_CLASSIFICATION,
                               VAR_CLIENT_CATEGORY, VAR_EMPLOYEE_ID,
                               VAR_END_DT, VAR_FRACTION, VAR_JOB_ID,
                               VAR_JOB_TYPE, VAR_ORG_UNIT_CD, VAR_POSITION_ID,
                               VAR_POSITION_INCREMENT, VAR_POSITION_START_DT,
                               VAR_POSITION_TITLE, VAR_START_DT,
                               VAR_STATUS_CD);

                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                       VAR_TRANSACTION_NAME,
                                       'ERROR',
                                       VAR_TABLE_NAME,
                                       VAR_KEY_DATA,
                                       'RECORD INSERT WHEN OTHERS EXCEPTION: '
                                       || SQLERRM);
                     RAISE;
               END;
            ELSE
               BEGIN
                  UPDATE EMP_EMPLOYEE_JOB_WK
                     SET AWARD = VAR_AWARD, CATEGORY_CD = VAR_CATEGORY_CD,
                         CLASSIFICATION = VAR_CLASSIFICATION,
                         CLIENT_CATEGORY = VAR_CLIENT_CATEGORY,
                         EMPLOYEE_ID = VAR_EMPLOYEE_ID, END_DT = VAR_END_DT,
                         FRACTION = VAR_FRACTION, JOB_ID = VAR_JOB_ID,
                         JOB_TYPE = VAR_JOB_TYPE,
                         ORG_UNIT_CD = VAR_ORG_UNIT_CD,
                         POSITION_ID = VAR_POSITION_ID,
                         POSITION_INCREMENT = VAR_POSITION_INCREMENT,
                         POSITION_START_DT = VAR_POSITION_START_DT,
                         POSITION_TITLE = VAR_POSITION_TITLE,
                         START_DT = VAR_START_DT, STATUS_CD = VAR_STATUS_CD
                   WHERE     EMPLOYEE_ID = VAR_EMPLOYEE_ID
                         AND JOB_ID = VAR_JOB_ID
                         AND START_DT = VAR_START_DT
                         AND POSITION_ID = VAR_POSITION_ID
                         AND JOB_TYPE = VAR_JOB_TYPE;

                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                       VAR_TRANSACTION_NAME,
                                       'ERROR',
                                       VAR_TABLE_NAME,
                                       VAR_KEY_DATA,
                                       'RECORD UPDATE WHEN OTHERS EXCEPTION: '
                                       || SQLERRM);
                     RAISE;
               END;
            END IF;
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      -- Unique-per-source
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                           VAR_TRANSACTION_NAME,
                           'ERROR',
                           VAR_TABLE_NAME,
                           VAR_KEY_DATA,
                           'WHEN OTHERS EXCEPTION: '
                           || SQLERRM);
         ROLLBACK;
   END EMP_SUBSTANTIVE;

   PROCEDURE EMP_SUBSTANTIVE_DEL
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR HR_CURSOR
      IS
         SELECT *
           FROM SUBSTANTIVE_DEL
          WHERE     SUBSTANTIVE_DEL.TP >= TRUNC(G_RUN_START)
                                          - 1
                AND SUBSTANTIVE_DEL.TP <= TRUNC(G_RUN_END)
                AND SUBSTANTIVE_DEL.TP_TYPE = 'D';

      VAR_DESCRIPTION        VARCHAR2(4000);
      VAR_KEY_DATA           VARCHAR2(4000);
      VAR_STATUS             VARCHAR2(4000);
      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'EMP_SUBSTANTIVE_DEL';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'EMP_EMPLOYEE_JOB_WK';
      VAR_EMPLOYEE#          VARCHAR2(4000);
      VAR_JOB#               VARCHAR2(4000);
      VAR_JOB_TYPE           VARCHAR2(4000);
      VAR_COMMENCE_DATE      DATE;
      VAR_POSITION#          VARCHAR2(4000);
      VAR_COUNT              VARCHAR2(4000);
      VAR_START_DT           DATE;
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Transaction Actions
      ------------------------------------------
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR HR_REC IN HR_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_EMPLOYEE# := HR_REC.EMPLOYEE#;
         VAR_JOB# := HR_REC.JOB#;
         VAR_JOB_TYPE := 'SUB';
         VAR_COMMENCE_DATE := HR_REC.COMMENCE_DATE;
         VAR_POSITION# := HR_REC.POSITION#;
         VAR_COUNT := 0;

         SELECT COUNT(*)
           INTO VAR_COUNT
           FROM SUBSTANTIVE
          WHERE     EMPLOYEE# = VAR_EMPLOYEE#
                AND JOB# = VAR_JOB#
                AND COMMENCE_DATE = VAR_COMMENCE_DATE
                AND POSITION# = VAR_POSITION#
                AND CLASSIFICATION NOT IN ('PRACT', 'SOC');

         IF VAR_COUNT > 0
         THEN
            VAR_STATUS := 'DISCARD';
            HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                              VAR_TRANSACTION_NAME,
                              'DEBUG',
                              VAR_TABLE_NAME,
                              VAR_KEY_DATA,
                              'RECORD DISCARD: '
                              || SQLERRM);
         ELSE
            VAR_STATUS := 'DELETE';
         END IF;

         VAR_START_DT := TRUNC(VAR_COMMENCE_DATE);

         VAR_STATUS := NULL;
         VAR_KEY_DATA :=
               'EMPLOYEE_ID: '
            || VAR_EMPLOYEE#
            || '|JOB_ID: '
            || VAR_JOB#
            || '|START_DT: '
            || VAR_START_DT
            || '|POSITION_ID: '
            || VAR_POSITION#
            || '|JOB_TYPE: '
            || VAR_JOB_TYPE;

         ------------------------------------------
         -- Constellar Hub : Actions = Insert, Update or Delete
         ------------------------------------------
         -- Perform insert, upsert or delete functions
         IF VAR_STATUS = 'DELETE'
         THEN
            BEGIN
               DELETE FROM EMP_EMPLOYEE_JOB_WK
                     WHERE     EMPLOYEE_ID = VAR_EMPLOYEE#
                           AND JOB_ID = VAR_JOB#
                           AND JOB_TYPE = VAR_JOB_TYPE
                           AND POSITION_ID = VAR_POSITION#
                           AND START_DT = VAR_START_DT;

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD DELETE WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      -- Unique-per-source
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                           VAR_TRANSACTION_NAME,
                           'ERROR',
                           VAR_TABLE_NAME,
                           VAR_KEY_DATA,
                           'WHEN OTHERS EXCEPTION: '
                           || SQLERRM);
         ROLLBACK;
   END EMP_SUBSTANTIVE_DEL;

   PROCEDURE EMP_CONCURRENT
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR HR_CURSOR
      IS
         SELECT *
           FROM CONCURRENT
          WHERE CONCURRENT.TP >= TRUNC(G_RUN_START)
                                 - 1
                AND CONCURRENT.TP <= TRUNC(G_RUN_END);

      VAR_DESCRIPTION          VARCHAR2(4000);
      VAR_KEY_DATA             VARCHAR2(4000);
      VAR_STATUS               VARCHAR2(4000);
      VAR_TRANSACTION_NAME     VARCHAR2(4000) := 'EMP_CONCURRENT';
      VAR_TABLE_NAME           VARCHAR2(4000) := 'EMP_EMPLOYEE_JOB_WK';
      VAR_POSITION             VARCHAR2(4000);
      VAR_AWARD                VARCHAR2(4000);
      VAR_CLEVEL               VARCHAR2(4000);
      VAR_ORG_UNIT             VARCHAR2(4000);
      VAR_CATEGORY_CD          VARCHAR2(4000);
      VAR_ORG_UNIT_CD          VARCHAR2(4000);
      VAR_CLASSIFICATION       VARCHAR2(4000);
      VAR_CLIENT_CATEGORY      VARCHAR2(4000);
      VAR_EMPLOYEE_ID          VARCHAR2(4000);
      VAR_END_DT               DATE;
      VAR_FRACTION             VARCHAR2(4000);
      VAR_JOB_ID               VARCHAR2(4000);
      VAR_JOB_TYPE             VARCHAR2(4000);
      VAR_POSITION_ID          VARCHAR2(4000);
      VAR_POSITION_INCREMENT   VARCHAR2(4000);
      VAR_COMMENCE             VARCHAR2(4000);
      VAR_START                DATE;
      VAR_POSITION_START_DT    DATE;
      VAR_POSITION_TITLE       VARCHAR2(4000);
      VAR_START_DT             DATE;
      VAR_STATUS_CD            VARCHAR2(4000);
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Transaction Actions
      ------------------------------------------

      FOR HR_REC IN HR_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : Pre Record Actions
         ------------------------------------------
         VAR_CLEVEL := HR_REC.CLEVEL;
         VAR_ORG_UNIT := NULL;

         BEGIN
            SELECT ORG_UNIT_CD
              INTO VAR_ORG_UNIT
              FROM EMP_ORG_UNIT
             WHERE ORG_UNIT_CD = VAR_CLEVEL AND END_DT IS NULL;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               VAR_ORG_UNIT := VAR_CLEVEL;
         END;

         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_AWARD := HR_REC.AWARD;

         IF VAR_AWARD IS NULL
         THEN
            VAR_AWARD := 'UNKWN';
         END IF;

         VAR_CATEGORY_CD := HR_REC.OCCUP_POS_CAT;
         VAR_CLASSIFICATION := HR_REC.CLASSIFICATION;
         VAR_CLIENT_CATEGORY := HR_REC.INOPERATIVE;
         VAR_EMPLOYEE_ID := HR_REC.EMPLOYEE#;
         VAR_END_DT := TRUNC(HR_REC.OCCUP_TERM_DATE);
         VAR_FRACTION := HR_REC.POS_FRACTION;
         VAR_JOB_ID := HR_REC.JOB#;
         VAR_JOB_TYPE := 'CON';
         VAR_ORG_UNIT_CD := VAR_ORG_UNIT;
         VAR_POSITION_ID := HR_REC.POSITION#;
         VAR_POSITION_INCREMENT := HR_REC.STEP#;

         VAR_POSITION := HR_REC.POSITION#;
         VAR_COMMENCE := HR_REC.COMMENCE_DATE;
         VAR_START := NULL;

         BEGIN
            SELECT NVL((SELECT MAX(START_DATE)
                          FROM POSITION
                         WHERE POSITION# = VAR_POSITION
                               AND START_DATE <= VAR_COMMENCE) --TO_DATE( var_commence , 'DD/MM/YYYY' ))
                                                              ,
                       (SELECT MIN(START_DATE)
                          FROM POSITION
                         WHERE POSITION# = VAR_POSITION))
                      AS START_DATE
              INTO VAR_START
              FROM DUAL;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               NULL;                                --var_start := '1/1/2000';
         END;

         VAR_POSITION_START_DT := VAR_START; --TO_DATE(var_start,"DD/MM/YYYY")
         VAR_POSITION_TITLE := HR_REC.OCCUP_POS_TITLE;
         VAR_START_DT := TRUNC(HR_REC.COMMENCE_DATE);
         VAR_STATUS_CD := HR_REC.EMP_STATUS;

         VAR_STATUS := NULL;
         VAR_KEY_DATA :=
               'EMPLOYEE_ID: '
            || VAR_EMPLOYEE_ID
            || '|JOB_ID: '
            || VAR_JOB_ID
            || '|START_DT: '
            || VAR_START_DT
            || '|POSITION_ID: '
            || VAR_POSITION_ID
            || '|JOB_TYPE: '
            || VAR_JOB_TYPE;

         -- Check if the target record existed
         BEGIN
            SELECT 'UPDATE'
              INTO VAR_STATUS
              FROM EMP_EMPLOYEE_JOB_WK
             WHERE     EMPLOYEE_ID = VAR_EMPLOYEE_ID
                   AND JOB_ID = VAR_JOB_ID
                   AND START_DT = VAR_START_DT
                   AND POSITION_ID = VAR_POSITION_ID
                   AND JOB_TYPE = VAR_JOB_TYPE;
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
               INSERT INTO EMP_EMPLOYEE_JOB_WK(AWARD, CATEGORY_CD,
                              CLASSIFICATION, CLIENT_CATEGORY, EMPLOYEE_ID,
                              END_DT, FRACTION, JOB_ID, JOB_TYPE, ORG_UNIT_CD,
                              POSITION_ID, POSITION_INCREMENT,
                              POSITION_START_DT, POSITION_TITLE, START_DT,
                              STATUS_CD)
                    VALUES (VAR_AWARD, VAR_CATEGORY_CD, VAR_CLASSIFICATION,
                            VAR_CLIENT_CATEGORY, VAR_EMPLOYEE_ID, VAR_END_DT,
                            VAR_FRACTION, VAR_JOB_ID, VAR_JOB_TYPE,
                            VAR_ORG_UNIT_CD, VAR_POSITION_ID,
                            VAR_POSITION_INCREMENT, VAR_POSITION_START_DT,
                            VAR_POSITION_TITLE, VAR_START_DT, VAR_STATUS_CD);

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD INSERT WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         ELSE
            BEGIN
               UPDATE EMP_EMPLOYEE_JOB_WK
                  SET AWARD = VAR_AWARD, CATEGORY_CD = VAR_CATEGORY_CD,
                      CLASSIFICATION = VAR_CLASSIFICATION,
                      CLIENT_CATEGORY = VAR_CLIENT_CATEGORY,
                      EMPLOYEE_ID = VAR_EMPLOYEE_ID, END_DT = VAR_END_DT,
                      FRACTION = VAR_FRACTION, JOB_ID = VAR_JOB_ID,
                      JOB_TYPE = VAR_JOB_TYPE, ORG_UNIT_CD = VAR_ORG_UNIT_CD,
                      POSITION_ID = VAR_POSITION_ID,
                      POSITION_INCREMENT = VAR_POSITION_INCREMENT,
                      POSITION_START_DT = VAR_POSITION_START_DT,
                      POSITION_TITLE = VAR_POSITION_TITLE,
                      START_DT = VAR_START_DT, STATUS_CD = VAR_STATUS_CD
                WHERE     EMPLOYEE_ID = VAR_EMPLOYEE_ID
                      AND JOB_ID = VAR_JOB_ID
                      AND START_DT = VAR_START_DT
                      AND POSITION_ID = VAR_POSITION_ID
                      AND JOB_TYPE = VAR_JOB_TYPE;

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD UPDATE WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      -- Unique-per-source
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                           VAR_TRANSACTION_NAME,
                           'ERROR',
                           VAR_TABLE_NAME,
                           VAR_KEY_DATA,
                           'WHEN OTHERS EXCEPTION: '
                           || SQLERRM);
         ROLLBACK;
   END EMP_CONCURRENT;

   PROCEDURE EMP_CONCURRENT_DEL
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR HR_CURSOR
      IS
         SELECT *
           FROM CONCURRENT_DEL
          WHERE     TP >= TRUNC(G_RUN_START)
                          - 1
                AND TP <= TRUNC(G_RUN_END)
                AND TP_TYPE = 'D';


      VAR_DESCRIPTION        VARCHAR2(4000);
      VAR_KEY_DATA           VARCHAR2(4000);
      VAR_STATUS             VARCHAR2(4000);
      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'EMP_CONCURRENT_DEL';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'EMP_EMPLOYEE_JOB_WK';
      VAR_EMPLOYEE#          VARCHAR2(4000);
      VAR_JOB#               VARCHAR2(4000);
      VAR_JOB_TYPE           VARCHAR2(4000);
      VAR_COMMENCE_DATE      DATE;
      VAR_POSITION#          VARCHAR2(4000);
      VAR_COUNT              VARCHAR2(4000);
      VAR_START_DT           DATE;
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Transaction Actions
      ------------------------------------------
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR HR_REC IN HR_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_EMPLOYEE# := HR_REC.EMPLOYEE#;
         VAR_JOB# := HR_REC.JOB#;
         VAR_JOB_TYPE := 'CON';
         VAR_COMMENCE_DATE := HR_REC.COMMENCE_DATE;
         VAR_POSITION# := HR_REC.POSITION#;
         VAR_COUNT := 0;

         SELECT COUNT(*)
           INTO VAR_COUNT
           FROM CONCURRENT
          WHERE     EMPLOYEE# = VAR_EMPLOYEE#
                AND JOB# = VAR_JOB#
                AND COMMENCE_DATE = VAR_COMMENCE_DATE
                AND POSITION# = VAR_POSITION#;

         IF VAR_COUNT > 0
         THEN
            VAR_STATUS := 'DISCARD';
            HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                              VAR_TRANSACTION_NAME,
                              'DEBUG',
                              VAR_TABLE_NAME,
                              VAR_KEY_DATA,
                              'RECORD DISCARD: '
                              || SQLERRM);
         ELSE
            VAR_STATUS := 'DELETE';
         END IF;

         VAR_START_DT := TRUNC(VAR_COMMENCE_DATE);

         VAR_STATUS := NULL;
         VAR_KEY_DATA :=
               'EMPLOYEE_ID: '
            || VAR_EMPLOYEE#
            || '|JOB_ID: '
            || VAR_JOB#
            || '|START_DT: '
            || VAR_START_DT
            || '|POSITION_ID: '
            || VAR_POSITION#
            || '|JOB_TYPE: '
            || VAR_JOB_TYPE;

         ------------------------------------------
         -- Constellar Hub : Actions = Insert, Update or Delete
         ------------------------------------------
         -- Perform insert, upsert or delete functions
         IF VAR_STATUS = 'DELETE'
         THEN
            BEGIN
               DELETE FROM EMP_EMPLOYEE_JOB_WK
                     WHERE     EMPLOYEE_ID = VAR_EMPLOYEE#
                           AND JOB_ID = VAR_JOB#
                           AND JOB_TYPE = VAR_JOB_TYPE
                           AND POSITION_ID = VAR_POSITION#
                           AND START_DT = VAR_START_DT;

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD DELETE WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      -- Unique-per-source
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                           VAR_TRANSACTION_NAME,
                           'ERROR',
                           VAR_TABLE_NAME,
                           VAR_KEY_DATA,
                           'WHEN OTHERS EXCEPTION: '
                           || SQLERRM);
         ROLLBACK;
   END EMP_CONCURRENT_DEL;

   PROCEDURE EMP_HDA
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR HR_CURSOR
      IS
         SELECT *
           FROM HDA
          WHERE TP >= TRUNC(G_RUN_START)
                      - 1
                AND TP <= TRUNC(G_RUN_END);

      VAR_KEY_DATA             VARCHAR2(4000);
      VAR_STATUS               VARCHAR2(4000);
      VAR_TRANSACTION_NAME     VARCHAR2(4000) := 'EMP_HDA';
      VAR_TABLE_NAME           VARCHAR2(4000) := 'EMP_EMPLOYEE_JOB_WK';
      VAR_CODE                 VARCHAR2(4000);
      VAR_DESCRIPTION          VARCHAR2(4000);
      VAR_KIND                 VARCHAR2(4000);
      VAR_PARENT_CODE          VARCHAR2(4000);
      VAR_PARENT_DESC          VARCHAR2(4000);
      VAR_POSITION             VARCHAR2(4000);
      VAR_AWARD                VARCHAR2(4000);
      VAR_CLEVEL               VARCHAR2(4000);
      VAR_ORG_UNIT             VARCHAR2(4000);
      VAR_CATEGORY_CD          VARCHAR2(4000);
      VAR_ORG_UNIT_CD          VARCHAR2(4000);
      VAR_CLASSIFICATION       VARCHAR2(4000);
      VAR_CLIENT_CATEGORY      VARCHAR2(4000);
      VAR_EMPLOYEE_ID          VARCHAR2(4000);
      VAR_END_DT               DATE;
      VAR_FRACTION             VARCHAR2(4000);
      VAR_JOB_ID               VARCHAR2(4000);
      VAR_JOB_TYPE             VARCHAR2(4000);
      VAR_POSITION_ID          VARCHAR2(4000);
      VAR_POSITION_INCREMENT   VARCHAR2(4000);
      VAR_COMMENCE             VARCHAR2(4000);
      VAR_START                DATE;
      VAR_POSITION_START_DT    DATE;
      VAR_POSITION_TITLE       VARCHAR2(4000);
      VAR_START_DT             DATE;
      VAR_STATUS_CD            VARCHAR2(4000);
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Transaction Actions
      ------------------------------------------

      FOR HR_REC IN HR_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : Pre Record Actions
         ------------------------------------------
         VAR_CLEVEL := HR_REC.CLEVEL;
         VAR_ORG_UNIT := NULL;

         BEGIN
            SELECT ORG_UNIT_CD
              INTO VAR_ORG_UNIT
              FROM EMP_ORG_UNIT
             WHERE ORG_UNIT_CD = VAR_CLEVEL AND END_DT IS NULL;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               VAR_ORG_UNIT := VAR_CLEVEL;
         END;

         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_AWARD := HR_REC.AWARD;

         IF VAR_AWARD IS NULL
         THEN
            VAR_AWARD := 'UNKWN';
         END IF;

         VAR_CATEGORY_CD := HR_REC.OCCUP_POS_CAT;
         VAR_CLASSIFICATION := HR_REC.CLASSIFICATION;
         VAR_CLIENT_CATEGORY := HR_REC.INOPERATIVE;
         VAR_EMPLOYEE_ID := HR_REC.EMPLOYEE#;
         VAR_END_DT := TRUNC(HR_REC.OCCUP_TERM_DATE);
         VAR_FRACTION := HR_REC.POS_FRACTION;
         VAR_JOB_ID := HR_REC.JOB#;
         VAR_JOB_TYPE := 'HDA';
         VAR_ORG_UNIT_CD := VAR_ORG_UNIT;
         VAR_POSITION_ID := HR_REC.POSITION#;
         VAR_POSITION_INCREMENT := HR_REC.STEP#;

         VAR_POSITION := HR_REC.POSITION#;
         VAR_COMMENCE := HR_REC.COMMENCE_DATE;
         VAR_START := NULL;

         BEGIN
            SELECT NVL((SELECT MAX(START_DATE)
                          FROM POSITION
                         WHERE POSITION# = VAR_POSITION
                               AND START_DATE <= VAR_COMMENCE) --TO_DATE( var_commence , 'DD/MM/YYYY' ))
                                                              ,
                       (SELECT MIN(START_DATE)
                          FROM POSITION
                         WHERE POSITION# = VAR_POSITION))
                      AS START_DATE
              INTO VAR_START
              FROM DUAL;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               NULL;                                --var_start := '1/1/2000';
         END;

         VAR_POSITION_START_DT := VAR_START; --TO_DATE(var_start,"DD/MM/YYYY")
         VAR_POSITION_TITLE := HR_REC.OCCUP_POS_TITLE;
         VAR_START_DT := TRUNC(HR_REC.COMMENCE_DATE);
         VAR_STATUS_CD := HR_REC.EMP_STATUS;

         VAR_STATUS := NULL;
         VAR_KEY_DATA :=
               'EMPLOYEE_ID: '
            || VAR_EMPLOYEE_ID
            || '|JOB_ID: '
            || VAR_JOB_ID
            || '|START_DT: '
            || VAR_START_DT
            || '|POSITION_ID: '
            || VAR_POSITION_ID
            || '|JOB_TYPE: '
            || VAR_JOB_TYPE;

         BEGIN
            SELECT 'UPDATE'               
              INTO VAR_STATUS
              FROM EMP_EMPLOYEE_JOB_WK
             WHERE     EMPLOYEE_ID = VAR_EMPLOYEE_ID
                   AND JOB_ID = VAR_JOB_ID
                   AND START_DT = VAR_START_DT
                   AND POSITION_ID = VAR_POSITION_ID
                   AND JOB_TYPE = VAR_JOB_TYPE;
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
               INSERT INTO EMP_EMPLOYEE_JOB_WK(AWARD, CATEGORY_CD,
                              CLASSIFICATION, CLIENT_CATEGORY, EMPLOYEE_ID,
                              END_DT, FRACTION, JOB_ID, JOB_TYPE, ORG_UNIT_CD,
                              POSITION_ID, POSITION_INCREMENT,
                              POSITION_START_DT, POSITION_TITLE, START_DT,
                              STATUS_CD)
                    VALUES (VAR_AWARD, VAR_CATEGORY_CD, VAR_CLASSIFICATION,
                            VAR_CLIENT_CATEGORY, VAR_EMPLOYEE_ID, VAR_END_DT,
                            VAR_FRACTION, VAR_JOB_ID, VAR_JOB_TYPE,
                            VAR_ORG_UNIT_CD, VAR_POSITION_ID,
                            VAR_POSITION_INCREMENT, VAR_POSITION_START_DT,
                            VAR_POSITION_TITLE, VAR_START_DT, VAR_STATUS_CD);

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD INSERT WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         ELSE
            BEGIN
               UPDATE EMP_EMPLOYEE_JOB_WK
                  SET AWARD = VAR_AWARD, CATEGORY_CD = VAR_CATEGORY_CD,
                      CLASSIFICATION = VAR_CLASSIFICATION,
                      CLIENT_CATEGORY = VAR_CLIENT_CATEGORY,
                      EMPLOYEE_ID = VAR_EMPLOYEE_ID, END_DT = VAR_END_DT,
                      FRACTION = VAR_FRACTION, JOB_ID = VAR_JOB_ID,
                      JOB_TYPE = VAR_JOB_TYPE, ORG_UNIT_CD = VAR_ORG_UNIT_CD,
                      POSITION_ID = VAR_POSITION_ID,
                      POSITION_INCREMENT = VAR_POSITION_INCREMENT,
                      POSITION_START_DT = VAR_POSITION_START_DT,
                      POSITION_TITLE = VAR_POSITION_TITLE,
                      START_DT = VAR_START_DT, STATUS_CD = VAR_STATUS_CD
                WHERE     EMPLOYEE_ID = VAR_EMPLOYEE_ID
                      AND JOB_ID = VAR_JOB_ID
                      AND START_DT = VAR_START_DT
                      AND POSITION_ID = VAR_POSITION_ID
                      AND JOB_TYPE = VAR_JOB_TYPE;

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD UPDATE WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      -- Unique-per-source
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                           VAR_TRANSACTION_NAME,
                           'ERROR',
                           VAR_TABLE_NAME,
                           VAR_KEY_DATA,
                           'WHEN OTHERS EXCEPTION: '
                           || SQLERRM);
         ROLLBACK;
   END EMP_HDA;


   PROCEDURE EMP_HDA_DEL
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR HR_CURSOR
      IS
         SELECT *
           FROM HDA_DEL
          WHERE     TP >= TRUNC(G_RUN_START)
                          - 1
                AND TP <= TRUNC(G_RUN_END)
                AND TP_TYPE = 'D';

      VAR_KEY_DATA           VARCHAR2(4000);
      VAR_STATUS             VARCHAR2(4000);
      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'EMP_HDA_DEL';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'EMP_EMPLOYEE_JOB_WK';
      VAR_EMPLOYEE#          VARCHAR2(4000);
      VAR_JOB#               VARCHAR2(4000);
      VAR_JOB_TYPE           VARCHAR2(4000);
      VAR_COMMENCE_DATE      DATE;
      VAR_POSITION#          VARCHAR2(4000);
      VAR_COUNT              VARCHAR2(4000);
      VAR_START_DT           DATE;
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Transaction Actions
      ------------------------------------------

      FOR HR_REC IN HR_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : Pre Record Actions
         ------------------------------------------
         VAR_EMPLOYEE# := HR_REC.EMPLOYEE#;
         VAR_JOB# := HR_REC.JOB#;
         VAR_COMMENCE_DATE := HR_REC.COMMENCE_DATE;
         VAR_POSITION# := HR_REC.POSITION#;
         VAR_JOB_TYPE := 'HDA';
         VAR_COUNT := 0;

         SELECT COUNT(*)
              INTO VAR_COUNT
              FROM HDA
             WHERE     EMPLOYEE# = VAR_EMPLOYEE#
                   AND JOB# = VAR_JOB#
                   AND COMMENCE_DATE = VAR_COMMENCE_DATE
                   AND POSITION# = VAR_POSITION#;

         IF VAR_COUNT > 0
         THEN
            VAR_STATUS := 'DISCARD';
            HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                              VAR_TRANSACTION_NAME,
                              'DEBUG',
                              VAR_TABLE_NAME,
                              VAR_KEY_DATA,
                              'RECORD DISCARD: '
                              || SQLERRM);
         ELSE
            VAR_STATUS := 'DELETE';
         END IF;

         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_START_DT := TRUNC(VAR_COMMENCE_DATE);

         VAR_STATUS := NULL;
         VAR_KEY_DATA :=
               'EMPLOYEE_ID: '
            || VAR_EMPLOYEE#
            || '|JOB_ID: '
            || VAR_JOB#
            || '|START_DT: '
            || VAR_START_DT
            || '|POSITION_ID: '
            || VAR_POSITION#
            || '|JOB_TYPE: '
            || VAR_JOB_TYPE;

         ------------------------------------------
         -- Constellar Hub : Actions = Insert, Update or Delete
         ------------------------------------------
         -- Perform insert, upsert or delete functions
         IF VAR_STATUS = 'DELETE'
         THEN
            BEGIN
               DELETE FROM EMP_EMPLOYEE_JOB_WK
                     WHERE     EMPLOYEE_ID = VAR_EMPLOYEE#
                           AND JOB_ID = VAR_JOB#
                           AND JOB_TYPE = VAR_JOB_TYPE
                           AND POSITION_ID = VAR_POSITION#
                           AND START_DT = VAR_START_DT;

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD DELETE WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      -- Unique-per-source
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                           VAR_TRANSACTION_NAME,
                           'ERROR',
                           VAR_TABLE_NAME,
                           VAR_KEY_DATA,
                           'WHEN OTHERS EXCEPTION: '
                           || SQLERRM);
         ROLLBACK;
   END EMP_HDA_DEL;

   PROCEDURE ORG_CLEVEL1
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR HR_CURSOR
      IS
         SELECT *
           FROM CODES
          WHERE KIND = 'CLEVEL1';

      VAR_KEY_DATA           VARCHAR2(4000);
      VAR_STATUS             VARCHAR2(4000);
      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'ORG_CLEVEL1';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'HR_ORG_TREE';
      VAR_CODE               VARCHAR2(4000);
      VAR_DESCRIPTION        VARCHAR2(4000);
      VAR_KIND               VARCHAR2(4000);
      VAR_PARENT_CODE        VARCHAR2(4000);
      VAR_PARENT_DESC        VARCHAR2(4000);
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Transaction Actions
      ------------------------------------------
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR HR_REC IN HR_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_CODE := HR_REC.CODE;
         VAR_DESCRIPTION := HR_REC.DESCRIPTION;
         VAR_KIND := HR_REC.KIND;
         VAR_PARENT_CODE := NULL;
         VAR_PARENT_DESC := NULL;

         VAR_STATUS := NULL;
         VAR_KEY_DATA :=
            'CODE: '
            || VAR_CODE;

         BEGIN
            SELECT 'UPDATE'                -- looks for active child org units
              INTO VAR_STATUS
              FROM HR_ORG_TREE
             WHERE CODE = VAR_CODE;
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
               INSERT INTO HR_ORG_TREE(CODE, DESCRIPTION, KIND, PARENT_CODE,
                              PARENT_DESC)
                    VALUES (VAR_CODE, VAR_DESCRIPTION, VAR_KIND,
                            VAR_PARENT_CODE, VAR_PARENT_DESC);

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD INSERT WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         ELSIF VAR_STATUS = 'UPDATE'
         THEN
            BEGIN
               UPDATE HR_ORG_TREE
                  SET CODE = VAR_CODE, DESCRIPTION = VAR_DESCRIPTION,
                      KIND = VAR_KIND, PARENT_CODE = VAR_PARENT_CODE,
                      PARENT_DESC = VAR_PARENT_DESC
                WHERE CODE = VAR_CODE;

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD UPDATE WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      -- Unique-per-source
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                           VAR_TRANSACTION_NAME,
                           'ERROR',
                           VAR_TABLE_NAME,
                           VAR_KEY_DATA,
                           'WHEN OTHERS EXCEPTION: '
                           || SQLERRM);
         ROLLBACK;
   END ORG_CLEVEL1;


   PROCEDURE ORG_CLEVEL23
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR HR_CURSOR
      IS
         SELECT CODES.*, PARENT.CODE AS PARENT_CODE,
                PARENT.DESCRIPTION AS PARENT_DESCRIPTION
           FROM CODES, CODES PARENT
          WHERE     TO_CHAR(SUBSTR(CODES.CODE,
                                   0,
                                   LENGTH(CODES.CODE)
                                   - 2)) = PARENT.CODE
                AND LENGTH(PARENT.KIND) = LENGTH(CODES.KIND)
                AND LENGTH(PARENT.KIND) = 7
                AND PARENT.KIND LIKE 'CLEVEL%'
                AND CODES.KIND LIKE 'CLEVEL%'
                AND LENGTH(CODES.CODE) <= 5;

      VAR_KEY_DATA           VARCHAR2(4000);
      VAR_STATUS             VARCHAR2(4000);
      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'ORG_CLEVEL23';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'HR_ORG_TREE';
      VAR_CODE               VARCHAR2(4000);
      VAR_DESCRIPTION        VARCHAR2(4000);
      VAR_KIND               VARCHAR2(4000);
      VAR_PARENT_CODE        VARCHAR2(4000);
      VAR_PARENT_DESC        VARCHAR2(4000);
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Transaction Actions
      ------------------------------------------
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR HR_REC IN HR_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_CODE := HR_REC.CODE;
         VAR_DESCRIPTION := HR_REC.DESCRIPTION;
         VAR_KIND := HR_REC.KIND;
         VAR_PARENT_CODE := HR_REC.PARENT_CODE;
         VAR_PARENT_DESC := HR_REC.PARENT_DESCRIPTION;

         VAR_STATUS := NULL;
         VAR_KEY_DATA :=
            'CODE: '
            || VAR_CODE;

         BEGIN
            SELECT 'UPDATE'                -- looks for active child org units
              INTO VAR_STATUS
              FROM HR_ORG_TREE
             WHERE CODE = VAR_CODE;
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
               INSERT INTO HR_ORG_TREE(CODE, DESCRIPTION, KIND, PARENT_CODE,
                              PARENT_DESC)
                    VALUES (VAR_CODE, VAR_DESCRIPTION, VAR_KIND,
                            VAR_PARENT_CODE, VAR_PARENT_DESC);

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD INSERT WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         ELSIF VAR_STATUS = 'UPDATE'
         THEN
            BEGIN
               UPDATE HR_ORG_TREE
                  SET CODE = VAR_CODE, DESCRIPTION = VAR_DESCRIPTION,
                      KIND = VAR_KIND, PARENT_CODE = VAR_PARENT_CODE,
                      PARENT_DESC = VAR_PARENT_DESC
                WHERE CODE = VAR_CODE;

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD UPDATE WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      -- Unique-per-source
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                           VAR_TRANSACTION_NAME,
                           'ERROR',
                           VAR_TABLE_NAME,
                           VAR_KEY_DATA,
                           'WHEN OTHERS EXCEPTION: '
                           || SQLERRM);
         ROLLBACK;
   END ORG_CLEVEL23;

   PROCEDURE ORG_CLEVEL45
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR HR_CURSOR
      IS
         SELECT CODES.*, PARENT.CODE AS PARENT_CODE,
                PARENT.DESCRIPTION AS PARENT_DESCRIPTION
           FROM CODES, CODES PARENT
          WHERE     TO_CHAR(SUBSTR(CODES.CODE,
                                   0,
                                   LENGTH(CODES.CODE)
                                   - 1)) = PARENT.CODE
                AND LENGTH(PARENT.KIND) = LENGTH(CODES.KIND)
                AND LENGTH(PARENT.KIND) = 7
                AND PARENT.KIND LIKE 'CLEVEL%'
                AND CODES.KIND LIKE 'CLEVEL%'
                AND LENGTH(CODES.CODE) > 5;

      VAR_KEY_DATA           VARCHAR2(4000);
      VAR_STATUS             VARCHAR2(4000);
      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'ORG_CLEVEL45';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'HR_ORG_TREE';
      VAR_CODE               VARCHAR2(4000);
      VAR_DESCRIPTION        VARCHAR2(4000);
      VAR_KIND               VARCHAR2(4000);
      VAR_PARENT_CODE        VARCHAR2(4000);
      VAR_PARENT_DESC        VARCHAR2(4000);
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Transaction Actions
      ------------------------------------------
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR HR_REC IN HR_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_CODE := HR_REC.CODE;
         VAR_DESCRIPTION := HR_REC.DESCRIPTION;
         VAR_KIND := HR_REC.KIND;
         VAR_PARENT_CODE := HR_REC.PARENT_CODE;
         VAR_PARENT_DESC := HR_REC.PARENT_DESCRIPTION;

         VAR_STATUS := NULL;
         VAR_KEY_DATA :=
            'CODE: '
            || VAR_CODE;

         BEGIN
            SELECT 'UPDATE'                -- looks for active child org units
              INTO VAR_STATUS
              FROM HR_ORG_TREE
             WHERE CODE = VAR_CODE;
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
               INSERT INTO HR_ORG_TREE(CODE, DESCRIPTION, KIND, PARENT_CODE,
                              PARENT_DESC)
                    VALUES (VAR_CODE, VAR_DESCRIPTION, VAR_KIND,
                            VAR_PARENT_CODE, VAR_PARENT_DESC);

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD INSERT WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         ELSIF VAR_STATUS = 'UPDATE'
         THEN
            BEGIN
               UPDATE HR_ORG_TREE
                  SET CODE = VAR_CODE, DESCRIPTION = VAR_DESCRIPTION,
                      KIND = VAR_KIND, PARENT_CODE = VAR_PARENT_CODE,
                      PARENT_DESC = VAR_PARENT_DESC
                WHERE CODE = VAR_CODE;

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD UPDATE WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      -- Unique-per-source
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                           VAR_TRANSACTION_NAME,
                           'ERROR',
                           VAR_TABLE_NAME,
                           VAR_KEY_DATA,
                           'WHEN OTHERS EXCEPTION: '
                           || SQLERRM);
         ROLLBACK;
   END ORG_CLEVEL45;

   PROCEDURE EMP_CLEVEL1
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR HR_CURSOR
      IS
         SELECT *
           FROM HR_ORG_TREE
          WHERE KIND = 'CLEVEL1';

      VAR_KEY_DATA             VARCHAR2(4000);
      VAR_STATUS               VARCHAR2(4000);
      VAR_TRANSACTION_NAME     VARCHAR2(4000) := 'EMP_CLEVEL1';
      VAR_TABLE_NAME           VARCHAR2(4000) := 'EMP_ORG_UNIT';
      VAR_END                  DATE;
      VAR_HIERARCHY_LEVEL      VARCHAR2(4000);
      VAR_ORG_UNIT_CD          VARCHAR2(4000);
      VAR_ORG_UNIT_DESC        VARCHAR2(4000);
      VAR_PARENT_ORG_UNIT_CD   VARCHAR2(4000);
      VAR_EMP_DESC             VARCHAR2(4000);
      VAR_QHO_DESC             VARCHAR2(4000);
      VAR_CODE                 VARCHAR2(4000);
      VAR_START                DATE;
      VAR_END_DT               DATE;
      VAR_COUNT                VARCHAR2(4000);
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Transaction Actions
      ------------------------------------------
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR HR_REC IN HR_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_EMP_DESC := NULL;
         VAR_QHO_DESC := HR_REC.DESCRIPTION;
         VAR_CODE := HR_REC.CODE;
         VAR_START := NULL;
         VAR_END := NULL;
         VAR_HIERARCHY_LEVEL := HR_REC.KIND;
         VAR_PARENT_ORG_UNIT_CD := HR_REC.PARENT_CODE;

         VAR_STATUS := NULL;
         VAR_KEY_DATA :=
            'ORG_UNIT_CD: '
            || VAR_CODE;

         BEGIN
            -- Check to see if this org_unit_cd exist in emp_org_unit.
            -- If exist grab the Description and start date.
            SELECT ORG_UNIT_DESC, START_DT, END_DT
              INTO VAR_EMP_DESC, VAR_START, VAR_END
              FROM EMP_ORG_UNIT
             WHERE ORG_UNIT_CD = VAR_CODE;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               VAR_EMP_DESC := NULL;
               VAR_START := NULL;
         END;

         IF VAR_EMP_DESC IS NULL AND VAR_START IS NULL
         THEN
            BEGIN
               SELECT NVL(MIN(START_DT), SYSDATE)
                 INTO VAR_START
                 FROM EMP_ORG_UNIT
                WHERE VAR_CODE = PARENT_ORG_UNIT_CD;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  VAR_START := SYSDATE;
            END;

            VAR_STATUS := 'CREATE';
         ELSIF VAR_EMP_DESC != VAR_QHO_DESC AND VAR_END IS NULL
         THEN
            VAR_STATUS := 'UPDATE';
         ELSE
            VAR_STATUS := 'DISCARD';
            HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                              VAR_TRANSACTION_NAME,
                              'DEBUG',
                              VAR_TABLE_NAME,
                              VAR_KEY_DATA,
                              'RECORD DISCARD: '
                              || SQLERRM);
         END IF;

         VAR_END_DT := NULL;
         VAR_COUNT := 0;

         BEGIN
            SELECT COUNT(*)                -- looks for active child org units
              INTO VAR_COUNT
              FROM EMP_ORG_UNIT EOUN
             WHERE EOUN.PARENT_ORG_UNIT_CD = VAR_CODE AND EOUN.END_DT IS NULL;

            IF VAR_COUNT > 0
            THEN
               VAR_END_DT := NULL;
            ELSE                                 --  no active child org units
               SELECT COUNT(*)       -- looks for any existing child org units
                 INTO VAR_COUNT
                 FROM EMP_ORG_UNIT EOUN
                WHERE EOUN.PARENT_ORG_UNIT_CD = VAR_CODE;

               IF VAR_COUNT < 1
               THEN
                  VAR_END_DT := SYSDATE;
               ELSE
                  SELECT MAX(END_DT)   -- grabs latest child org unit end date
                    INTO VAR_END_DT
                    FROM EMP_ORG_UNIT EOUM
                   WHERE EOUM.PARENT_ORG_UNIT_CD = VAR_CODE;
               END IF;
            END IF;

            VAR_END := VAR_END_DT;
         END;

         ------------------------------------------
         -- Constellar Hub : Actions = Insert, Update or Delete
         ------------------------------------------
         -- Perform insert, upsert or delete functions
         IF VAR_STATUS = 'CREATE'
         THEN
            BEGIN
               INSERT INTO EMP_ORG_UNIT(END_DT, HIERARCHY_LEVEL, ORG_UNIT_CD,
                              ORG_UNIT_DESC, PARENT_ORG_UNIT_CD, START_DT)
                    VALUES (VAR_END, VAR_HIERARCHY_LEVEL, VAR_CODE,
                            VAR_QHO_DESC, VAR_PARENT_ORG_UNIT_CD, VAR_START);

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD INSERT WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         ELSIF VAR_STATUS = 'UPDATE'
         THEN
            BEGIN
               UPDATE EMP_ORG_UNIT
                  SET END_DT = VAR_END, HIERARCHY_LEVEL = VAR_HIERARCHY_LEVEL,
                      ORG_UNIT_CD = VAR_CODE, ORG_UNIT_DESC = VAR_QHO_DESC,
                      PARENT_ORG_UNIT_CD = VAR_PARENT_ORG_UNIT_CD,
                      START_DT = VAR_START
                WHERE ORG_UNIT_CD = VAR_CODE;

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD UPDATE WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      -- Unique-per-source
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                           VAR_TRANSACTION_NAME,
                           'ERROR',
                           VAR_TABLE_NAME,
                           VAR_KEY_DATA,
                           'WHEN OTHERS EXCEPTION: '
                           || SQLERRM);
         ROLLBACK;
   END EMP_CLEVEL1;


   PROCEDURE EMP_CLEVEL2
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR HR_CURSOR
      IS
         SELECT *
           FROM HR_ORG_TREE
          WHERE KIND = 'CLEVEL2';

      VAR_KEY_DATA             VARCHAR2(4000);
      VAR_STATUS               VARCHAR2(4000);
      VAR_TRANSACTION_NAME     VARCHAR2(4000) := 'EMP_CLEVEL2';
      VAR_TABLE_NAME           VARCHAR2(4000) := 'EMP_ORG_UNIT';
      VAR_END                  DATE;
      VAR_HIERARCHY_LEVEL      VARCHAR2(4000);
      VAR_ORG_UNIT_CD          VARCHAR2(4000);
      VAR_ORG_UNIT_DESC        VARCHAR2(4000);
      VAR_PARENT_ORG_UNIT_CD   VARCHAR2(4000);
      VAR_EMP_DESC             VARCHAR2(4000);
      VAR_QHO_DESC             VARCHAR2(4000);
      VAR_CODE                 VARCHAR2(4000);
      VAR_START                DATE;
      VAR_END_DT               DATE;
      VAR_COUNT                VARCHAR2(4000);
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Transaction Actions
      ------------------------------------------
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR HR_REC IN HR_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_EMP_DESC := NULL;
         VAR_QHO_DESC := HR_REC.DESCRIPTION;
         VAR_CODE := HR_REC.CODE;
         VAR_START := NULL;
         VAR_END := NULL;
         VAR_HIERARCHY_LEVEL := HR_REC.KIND;
         VAR_PARENT_ORG_UNIT_CD := HR_REC.PARENT_CODE;

         VAR_STATUS := NULL;
         VAR_KEY_DATA :=
            'ORG_UNIT_CD: '
            || VAR_CODE;

         BEGIN
            -- Check to see if this org_unit_cd exist in emp_org_unit.
            -- If exist grab the Description and start date.
            SELECT ORG_UNIT_DESC, START_DT, END_DT
              INTO VAR_EMP_DESC, VAR_START, VAR_END
              FROM EMP_ORG_UNIT
             WHERE ORG_UNIT_CD = VAR_CODE;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               VAR_EMP_DESC := NULL;
               VAR_START := NULL;
         END;

         IF VAR_EMP_DESC IS NULL AND VAR_START IS NULL
         THEN
            BEGIN
               SELECT NVL(MIN(START_DT), SYSDATE)
                 INTO VAR_START
                 FROM EMP_ORG_UNIT
                WHERE VAR_CODE = PARENT_ORG_UNIT_CD;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  VAR_START := SYSDATE;
            END;

            VAR_STATUS := 'CREATE';
         ELSIF VAR_EMP_DESC != VAR_QHO_DESC AND VAR_END IS NULL
         THEN
            VAR_STATUS := 'UPDATE';
         ELSE
            VAR_STATUS := 'DISCARD';
            HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                              VAR_TRANSACTION_NAME,
                              'DEBUG',
                              VAR_TABLE_NAME,
                              VAR_KEY_DATA,
                              'RECORD DISCARD: '
                              || SQLERRM);
         END IF;

         BEGIN
            VAR_END_DT := NULL;
            VAR_COUNT := 0;

            BEGIN
               SELECT COUNT(*)             -- looks for active child org units
                 INTO VAR_COUNT
                 FROM EMP_ORG_UNIT EOUN
                WHERE EOUN.PARENT_ORG_UNIT_CD = VAR_CODE
                      AND EOUN.END_DT IS NULL;

               IF VAR_COUNT > 0
               THEN
                  VAR_END_DT := NULL;
               ELSE                              --  no active child org units
                  SELECT COUNT(*)    -- looks for any existing child org units
                    INTO VAR_COUNT
                    FROM EMP_ORG_UNIT EOUN
                   WHERE EOUN.PARENT_ORG_UNIT_CD = VAR_CODE;

                  IF VAR_COUNT < 1
                  THEN
                     VAR_END_DT := SYSDATE;
                  ELSE
                     SELECT MAX(END_DT) -- grabs latest child org unit end date
                       INTO VAR_END_DT
                       FROM EMP_ORG_UNIT EOUM
                      WHERE EOUM.PARENT_ORG_UNIT_CD = VAR_CODE;
                  END IF;
               END IF;
            END;

            VAR_END := VAR_END_DT;
         END;

         ------------------------------------------
         -- Constellar Hub : Actions = Insert, Update or Delete
         ------------------------------------------
         -- Perform insert, upsert or delete functions
         IF VAR_STATUS = 'CREATE'
         THEN
            BEGIN
               INSERT INTO EMP_ORG_UNIT(END_DT, HIERARCHY_LEVEL, ORG_UNIT_CD,
                              ORG_UNIT_DESC, PARENT_ORG_UNIT_CD, START_DT)
                    VALUES (VAR_END, VAR_HIERARCHY_LEVEL, VAR_CODE,
                            VAR_QHO_DESC, VAR_PARENT_ORG_UNIT_CD, VAR_START);

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD INSERT WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         ELSIF VAR_STATUS = 'UPDATE'
         THEN
            BEGIN
               UPDATE EMP_ORG_UNIT
                  SET END_DT = VAR_END, HIERARCHY_LEVEL = VAR_HIERARCHY_LEVEL,
                      ORG_UNIT_CD = VAR_CODE, ORG_UNIT_DESC = VAR_QHO_DESC,
                      PARENT_ORG_UNIT_CD = VAR_PARENT_ORG_UNIT_CD,
                      START_DT = VAR_START
                WHERE ORG_UNIT_CD = VAR_CODE;

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD UPDATE WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      -- Unique-per-source
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                           VAR_TRANSACTION_NAME,
                           'ERROR',
                           VAR_TABLE_NAME,
                           VAR_KEY_DATA,
                           'WHEN OTHERS EXCEPTION: '
                           || SQLERRM);
         ROLLBACK;
   END EMP_CLEVEL2;

   PROCEDURE EMP_CLEVEL3
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR HR_CURSOR
      IS
         SELECT *
           FROM HR_ORG_TREE
          WHERE KIND = 'CLEVEL3';

      VAR_KEY_DATA             VARCHAR2(4000);
      VAR_STATUS               VARCHAR2(4000);
      VAR_TRANSACTION_NAME     VARCHAR2(4000) := 'EMP_CLEVEL3';
      VAR_TABLE_NAME           VARCHAR2(4000) := 'EMP_ORG_UNIT';
      VAR_END                  DATE;
      VAR_HIERARCHY_LEVEL      VARCHAR2(4000);
      VAR_ORG_UNIT_CD          VARCHAR2(4000);
      VAR_ORG_UNIT_DESC        VARCHAR2(4000);
      VAR_PARENT_ORG_UNIT_CD   VARCHAR2(4000);
      VAR_EMP_DESC             VARCHAR2(4000);
      VAR_QHO_DESC             VARCHAR2(4000);
      VAR_CODE                 VARCHAR2(4000);
      VAR_START                DATE;
      VAR_END_DT               DATE;
      VAR_COUNT                VARCHAR2(4000);
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Transaction Actions
      ------------------------------------------
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR HR_REC IN HR_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_EMP_DESC := NULL;
         VAR_QHO_DESC := HR_REC.DESCRIPTION;
         VAR_CODE := HR_REC.CODE;
         VAR_START := NULL;
         VAR_END := NULL;
         VAR_HIERARCHY_LEVEL := HR_REC.KIND;
         VAR_PARENT_ORG_UNIT_CD := HR_REC.PARENT_CODE;

         VAR_STATUS := NULL;
         VAR_KEY_DATA :=
            'ORG_UNIT_CD: '
            || VAR_CODE;

         BEGIN
            -- Check to see if this org_unit_cd exist in emp_org_unit.
            -- If exist grab the Description and start date.
            SELECT ORG_UNIT_DESC, START_DT, END_DT
              INTO VAR_EMP_DESC, VAR_START, VAR_END
              FROM EMP_ORG_UNIT
             WHERE ORG_UNIT_CD = VAR_CODE;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               VAR_EMP_DESC := NULL;
               VAR_START := NULL;
         END;

         IF VAR_EMP_DESC IS NULL AND VAR_START IS NULL
         THEN
            BEGIN
               SELECT NVL(MIN(START_DT), SYSDATE)
                 INTO VAR_START
                 FROM EMP_ORG_UNIT
                WHERE VAR_CODE = PARENT_ORG_UNIT_CD;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  VAR_START := SYSDATE;
            END;

            VAR_STATUS := 'CREATE';
         ELSIF VAR_EMP_DESC != VAR_QHO_DESC AND VAR_END IS NULL
         THEN
            VAR_STATUS := 'UPDATE';
         ELSE
            VAR_STATUS := 'DISCARD';
            HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                              VAR_TRANSACTION_NAME,
                              'DEBUG',
                              VAR_TABLE_NAME,
                              VAR_KEY_DATA,
                              'RECORD DISCARD: '
                              || SQLERRM);
         END IF;

         BEGIN
            VAR_END_DT := NULL;
            VAR_COUNT := 0;

            BEGIN
               SELECT COUNT(*)             -- looks for active child org units
                 INTO VAR_COUNT
                 FROM EMP_ORG_UNIT EOUN
                WHERE EOUN.PARENT_ORG_UNIT_CD = VAR_CODE
                      AND EOUN.END_DT IS NULL;

               IF VAR_COUNT > 0
               THEN
                  VAR_END_DT := NULL;
               ELSE                              --  no active child org units
                  SELECT COUNT(*)    -- looks for any existing child org units
                    INTO VAR_COUNT
                    FROM EMP_ORG_UNIT EOUN
                   WHERE EOUN.PARENT_ORG_UNIT_CD = VAR_CODE;

                  IF VAR_COUNT < 1
                  THEN
                     VAR_END_DT := SYSDATE;
                  ELSE
                     SELECT MAX(END_DT) -- grabs latest child org unit end date
                       INTO VAR_END_DT
                       FROM EMP_ORG_UNIT EOUM
                      WHERE EOUM.PARENT_ORG_UNIT_CD = VAR_CODE;
                  END IF;
               END IF;
            END;

            VAR_END := VAR_END_DT;
         END;

         ------------------------------------------
         -- Constellar Hub : Actions = Insert, Update or Delete
         ------------------------------------------
         -- Perform insert, upsert or delete functions
         IF VAR_STATUS = 'CREATE'
         THEN
            BEGIN
               INSERT INTO EMP_ORG_UNIT(END_DT, HIERARCHY_LEVEL, ORG_UNIT_CD,
                              ORG_UNIT_DESC, PARENT_ORG_UNIT_CD, START_DT)
                    VALUES (VAR_END, VAR_HIERARCHY_LEVEL, VAR_CODE,
                            VAR_QHO_DESC, VAR_PARENT_ORG_UNIT_CD, VAR_START);

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD INSERT WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         ELSIF VAR_STATUS = 'UPDATE'
         THEN
            BEGIN
               UPDATE EMP_ORG_UNIT
                  SET END_DT = VAR_END, HIERARCHY_LEVEL = VAR_HIERARCHY_LEVEL,
                      ORG_UNIT_CD = VAR_CODE, ORG_UNIT_DESC = VAR_QHO_DESC,
                      PARENT_ORG_UNIT_CD = VAR_PARENT_ORG_UNIT_CD,
                      START_DT = VAR_START
                WHERE ORG_UNIT_CD = VAR_CODE;

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD UPDATE WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      -- Unique-per-source
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                           VAR_TRANSACTION_NAME,
                           'ERROR',
                           VAR_TABLE_NAME,
                           VAR_KEY_DATA,
                           'WHEN OTHERS EXCEPTION: '
                           || SQLERRM);
         ROLLBACK;
   END EMP_CLEVEL3;

   PROCEDURE EMP_CLEVEL4
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR HR_CURSOR
      IS
         SELECT *
           FROM HR_ORG_TREE
          WHERE KIND = 'CLEVEL4';

      VAR_KEY_DATA             VARCHAR2(4000);
      VAR_STATUS               VARCHAR2(4000);
      VAR_TRANSACTION_NAME     VARCHAR2(4000) := 'EMP_CLEVEL4';
      VAR_TABLE_NAME           VARCHAR2(4000) := 'EMP_ORG_UNIT';
      VAR_END                  DATE;
      VAR_HIERARCHY_LEVEL      VARCHAR2(4000);
      VAR_ORG_UNIT_CD          VARCHAR2(4000);
      VAR_ORG_UNIT_DESC        VARCHAR2(4000);
      VAR_PARENT_ORG_UNIT_CD   VARCHAR2(4000);
      VAR_EMP_DESC             VARCHAR2(4000);
      VAR_QHO_DESC             VARCHAR2(4000);
      VAR_CODE                 VARCHAR2(4000);
      VAR_START                DATE;
      VAR_END_DT               DATE;
      VAR_COUNT                VARCHAR2(4000);
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Transaction Actions
      ------------------------------------------
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR HR_REC IN HR_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_EMP_DESC := NULL;
         VAR_QHO_DESC := HR_REC.DESCRIPTION;
         VAR_CODE := HR_REC.CODE;
         VAR_START := NULL;
         VAR_END := NULL;
         VAR_HIERARCHY_LEVEL := HR_REC.KIND;
         VAR_PARENT_ORG_UNIT_CD := HR_REC.PARENT_CODE;

         VAR_STATUS := NULL;
         VAR_KEY_DATA :=
            'ORG_UNIT_CD: '
            || VAR_CODE;

         BEGIN
            -- Check to see if this org_unit_cd exist in emp_org_unit.
            -- If exist grab the Description and start date.
            SELECT ORG_UNIT_DESC, START_DT, END_DT
              INTO VAR_EMP_DESC, VAR_START, VAR_END
              FROM EMP_ORG_UNIT
             WHERE ORG_UNIT_CD = VAR_CODE;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               VAR_EMP_DESC := NULL;
               VAR_START := NULL;
         END;

         IF VAR_EMP_DESC IS NULL AND VAR_START IS NULL
         THEN
            BEGIN
               SELECT NVL(MIN(START_DT), SYSDATE)
                 INTO VAR_START
                 FROM EMP_ORG_UNIT
                WHERE VAR_CODE = PARENT_ORG_UNIT_CD;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  VAR_START := SYSDATE;
            END;

            VAR_STATUS := 'CREATE';
         ELSIF VAR_EMP_DESC != VAR_QHO_DESC AND VAR_END IS NULL
         THEN
            VAR_STATUS := 'UPDATE';
         ELSE
            VAR_STATUS := 'DISCARD';
            HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                              VAR_TRANSACTION_NAME,
                              'DEBUG',
                              VAR_TABLE_NAME,
                              VAR_KEY_DATA,
                              'RECORD DISCARD: '
                              || SQLERRM);
         END IF;

         BEGIN
            VAR_END_DT := NULL;
            VAR_COUNT := 0;

            BEGIN
               SELECT COUNT(*)             -- looks for active child org units
                 INTO VAR_COUNT
                 FROM EMP_ORG_UNIT EOUN
                WHERE EOUN.PARENT_ORG_UNIT_CD = VAR_CODE
                      AND EOUN.END_DT IS NULL;

               IF VAR_COUNT > 0
               THEN
                  VAR_END_DT := NULL;
               ELSE                              --  no active child org units
                  SELECT COUNT(*)    -- looks for any existing child org units
                    INTO VAR_COUNT
                    FROM EMP_ORG_UNIT EOUN
                   WHERE EOUN.PARENT_ORG_UNIT_CD = VAR_CODE;

                  IF VAR_COUNT < 1
                  THEN
                     VAR_END_DT := SYSDATE;
                  ELSE
                     SELECT MAX(END_DT) -- grabs latest child org unit end date
                       INTO VAR_END_DT
                       FROM EMP_ORG_UNIT EOUM
                      WHERE EOUM.PARENT_ORG_UNIT_CD = VAR_CODE;
                  END IF;
               END IF;
            END;

            VAR_END := VAR_END_DT;
         END;

         -- WKT QA: Do we need to do the select to check if the record with the key exists? Up till this point if VAR_STATUS = 'UPDATE' that does not mean that record exist.
         -- WKT QA: Maybe the select should be in label:select  be VAR_STATUS = UPDATE is decided.
         ------------------------------------------
         -- Constellar Hub : Actions = Insert, Update or Delete
         ------------------------------------------
         -- Perform insert, upsert or delete functions
         IF VAR_STATUS = 'CREATE'
         THEN
            BEGIN
               INSERT INTO EMP_ORG_UNIT(END_DT, HIERARCHY_LEVEL, ORG_UNIT_CD,
                              ORG_UNIT_DESC, PARENT_ORG_UNIT_CD, START_DT)
                    VALUES (VAR_END, VAR_HIERARCHY_LEVEL, VAR_CODE,
                            VAR_QHO_DESC, VAR_PARENT_ORG_UNIT_CD, VAR_START);

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD INSERT WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         ELSIF VAR_STATUS = 'UPDATE'
         THEN
            BEGIN
               UPDATE EMP_ORG_UNIT
                  SET END_DT = VAR_END, HIERARCHY_LEVEL = VAR_HIERARCHY_LEVEL,
                      ORG_UNIT_CD = VAR_CODE, ORG_UNIT_DESC = VAR_QHO_DESC,
                      PARENT_ORG_UNIT_CD = VAR_PARENT_ORG_UNIT_CD,
                      START_DT = VAR_START
                WHERE ORG_UNIT_CD = VAR_CODE;

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD UPDATE WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      -- Unique-per-source
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                           VAR_TRANSACTION_NAME,
                           'ERROR',
                           VAR_TABLE_NAME,
                           VAR_KEY_DATA,
                           'WHEN OTHERS EXCEPTION: '
                           || SQLERRM);
         ROLLBACK;
   END EMP_CLEVEL4;

   PROCEDURE EMP_CLEVEL5
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR HR_CURSOR
      IS
         SELECT *
           FROM HR_ORG_TREE
          WHERE KIND = 'CLEVEL5';

      VAR_KEY_DATA             VARCHAR2(4000);
      VAR_STATUS               VARCHAR2(4000);
      VAR_TRANSACTION_NAME     VARCHAR2(4000) := 'EMP_CLEVEL5';
      VAR_TABLE_NAME           VARCHAR2(4000) := 'EMP_ORG_UNIT';
      VAR_END                  DATE;
      VAR_HIERARCHY_LEVEL      VARCHAR2(4000);
      VAR_ORG_UNIT_CD          VARCHAR2(4000);
      VAR_ORG_UNIT_DESC        VARCHAR2(4000);
      VAR_PARENT_ORG_UNIT_CD   VARCHAR2(4000);
      VAR_EMP_DESC             VARCHAR2(4000);
      VAR_QHO_DESC             VARCHAR2(4000);
      VAR_CODE                 VARCHAR2(4000);
      VAR_START                DATE;
      VAR_END_DT               DATE;
      VAR_EMP_START            DATE;
      VAR_EMP_END              DATE;
      VAR_SCL_START            DATE;
      VAR_SCL_ENDED            DATE;
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Transaction Actions
      ------------------------------------------
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR HR_REC IN HR_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_EMP_DESC := NULL;
         VAR_QHO_DESC := HR_REC.DESCRIPTION;
         VAR_CODE := HR_REC.CODE;
         VAR_START := NULL;
         VAR_END := NULL;
         VAR_HIERARCHY_LEVEL := HR_REC.KIND;
         VAR_PARENT_ORG_UNIT_CD := HR_REC.PARENT_CODE;

         VAR_STATUS := NULL;
         VAR_KEY_DATA :=
            'ORG_UNIT_CD: '
            || VAR_CODE;

         BEGIN
            -- Check to see if this org_unit_cd exist in emp_org_unit.
            -- If exist grab the Description and start date.
            SELECT ORG_UNIT_DESC, START_DT, END_DT
              INTO VAR_EMP_DESC, VAR_EMP_START, VAR_EMP_END
              FROM EMP_ORG_UNIT
             WHERE ORG_UNIT_CD = VAR_CODE;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               VAR_EMP_DESC := NULL;
               VAR_EMP_START := NULL;
               VAR_EMP_END := NULL;
         END;

         BEGIN
            -- Check for discontinued records in short_clevel to be propogated over
            SELECT SCL.END_DATE, SCL.START_DATE
              INTO VAR_SCL_ENDED, VAR_SCL_START
              FROM SHORT_CLEVEL SCL
             WHERE SCL.CLEVEL = VAR_CODE;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               VAR_SCL_ENDED := NULL;
         END;

         -- Set the emp_org_unit_end date to scl end date
         VAR_END := VAR_SCL_ENDED;

         -- If record does not exist in emp_org_unit
         IF     VAR_EMP_DESC IS NULL
            AND VAR_EMP_START IS NULL
            AND VAR_EMP_END IS NULL
         THEN
            VAR_START := VAR_SCL_START;
            VAR_STATUS := 'CREATE';
         -- Compare description
         ELSIF VAR_EMP_DESC != VAR_QHO_DESC AND VAR_EMP_END IS NULL
         THEN
            VAR_START := VAR_EMP_START;
            VAR_STATUS := 'UPDATE';
         -- **Changed from DISCARD to UPDATE at QV's request 22/09/08**
         -- If not ended in SCL and ended in QV
         ELSIF VAR_SCL_ENDED IS NULL AND VAR_EMP_END IS NOT NULL
         THEN
            --    RAISE ERROR "Unit "||var_code||" ended in QV but still valid in short_clevel. Record Discarded."
            --    DISCARD RECORD
            VAR_START := VAR_SCL_START;
            VAR_STATUS := 'UPDATE';
         ELSE
            VAR_STATUS := 'DISCARD';
            HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                              VAR_TRANSACTION_NAME,
                              'DEBUG',
                              VAR_TABLE_NAME,
                              VAR_KEY_DATA,
                              'RECORD DISCARD: '
                              || SQLERRM);
         END IF;

         ------------------------------------------
         -- Constellar Hub : Actions = Insert, Update or Delete
         ------------------------------------------
         -- Perform insert, upsert or delete functions
         IF VAR_STATUS = 'CREATE'
         THEN
            BEGIN
               INSERT INTO EMP_ORG_UNIT(END_DT, HIERARCHY_LEVEL, ORG_UNIT_CD,
                              ORG_UNIT_DESC, PARENT_ORG_UNIT_CD, START_DT)
                    VALUES (VAR_END, VAR_HIERARCHY_LEVEL, VAR_CODE,
                            VAR_QHO_DESC, VAR_PARENT_ORG_UNIT_CD, VAR_START);

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD INSERT WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         ELSIF VAR_STATUS = 'UPDATE'
         THEN
            BEGIN
               UPDATE EMP_ORG_UNIT
                  SET END_DT = VAR_END, HIERARCHY_LEVEL = VAR_HIERARCHY_LEVEL,
                      ORG_UNIT_CD = VAR_CODE, ORG_UNIT_DESC = VAR_QHO_DESC,
                      PARENT_ORG_UNIT_CD = VAR_PARENT_ORG_UNIT_CD,
                      START_DT = VAR_START
                WHERE ORG_UNIT_CD = VAR_CODE;

               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
            EXCEPTION
               WHEN OTHERS
               THEN
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                    VAR_TRANSACTION_NAME,
                                    'ERROR',
                                    VAR_TABLE_NAME,
                                    VAR_KEY_DATA,
                                    'RECORD UPDATE WHEN OTHERS EXCEPTION: '
                                    || SQLERRM);
                  RAISE;
            END;
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      -- Unique-per-source
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                           VAR_TRANSACTION_NAME,
                           'ERROR',
                           VAR_TABLE_NAME,
                           VAR_KEY_DATA,
                           'WHEN OTHERS EXCEPTION: '
                           || SQLERRM);
         ROLLBACK;
   END EMP_CLEVEL5;

   PROCEDURE POST_INTERFACE
   IS
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Post Interface Rule
      ------------------------------------------
      BEGIN
         DELETE FROM EMP_EMPLOYEE_JOB_UPD_WK;

         COMMIT;

         INSERT INTO EMP_EMPLOYEE_JOB_UPD_WK
            (                                  -- changes in Substantive Table
             SELECT  TO_NUMBER(EMPLOYEE#)
                FROM SUBSTANTIVE
               WHERE TP >= G_RUN_START
                           - 1
                     AND TP <= G_RUN_END
                     AND CLASSIFICATION NOT IN ('PRACT', 'SOC')
             UNION
             -- changes in Concurrent Table
             SELECT TO_NUMBER(EMPLOYEE#)
               FROM CONCURRENT
              WHERE TP >= G_RUN_START
                          - 1
                    AND TP <= G_RUN_END
             UNION
             -- changes in HDA Table
             SELECT TO_NUMBER(EMPLOYEE#)
               FROM HDA
              WHERE TP >= G_RUN_START
                          - 1
                    AND TP <= G_RUN_END
             UNION
             -- changes in Substantive Deletes Table
             SELECT TO_NUMBER(EMPLOYEE#)
               FROM SUBSTANTIVE_DEL
              WHERE     TP >= G_RUN_START
                              - 1
                    AND TP <= G_RUN_END
                    AND TP_TYPE = 'D'
             UNION
             -- changes in Concurrent Deletes Table
             SELECT TO_NUMBER(EMPLOYEE#)
               FROM CONCURRENT_DEL
              WHERE     TP >= G_RUN_START
                              - 1
                    AND TP <= G_RUN_END
                    AND TP_TYPE = 'D'
             UNION
             -- changes in HDA Deletes Table
             SELECT TO_NUMBER(EMPLOYEE#)
               FROM HDA_DEL
              WHERE     TP >= G_RUN_START
                              - 1
                    AND TP <= G_RUN_END
                    AND TP_TYPE = 'D'
             UNION
             -- Changes in Position Table , Added 10/5/2006
             -- 18 Aug 2006, W.M.Ho, missing records due to update_on is after RUN_END
             -- change RUN_END to sysdate
             SELECT EMPLOYEE_ID
               FROM EMP_EMPLOYEE_JOB_WK
              WHERE END_DT >= SYSDATE
                    AND POSITION_ID IN
                           (SELECT POSITION_ID
                              FROM EMP_POSITION
                             WHERE TRUNC(UPDATE_ON) >= G_RUN_START
                                                       - 1
                                   AND TRUNC(UPDATE_ON) <= SYSDATE)
             UNION
             -- Changes in Org Units Table, Added 10/5/2006
             SELECT EMPLOYEE_ID
               FROM EMP_EMPLOYEE_JOB_WK
              WHERE END_DT >= SYSDATE
                    AND ORG_UNIT_CD IN
                           (SELECT ORG_UNIT_CD
                              FROM EMP_ORG_UNIT
                             WHERE TRUNC(UPDATE_ON) >= G_RUN_START
                                                       - 1
                                   AND TRUNC(UPDATE_ON) <= SYSDATE));

         COMMIT;
      END;

      -- adding emp_org_units in employee table but not hr tables
      DECLARE
         CURSOR MOU
         IS
            (SELECT ORG_UNIT_CD FROM EMP_EMPLOYEE_JOB_WK
             MINUS
             SELECT ORG_UNIT_CD FROM EMP_ORG_UNIT);
      BEGIN
         FOR REC IN MOU
         LOOP
            BEGIN
               INSERT INTO EMP_ORG_UNIT(ORG_UNIT_CD, START_DT, END_DT,
                              ORG_UNIT_DESC, HIERARCHY_LEVEL)
                    VALUES (REC.ORG_UNIT_CD, SYSDATE, SYSDATE, 'UNKNOWN',
                            'CLEVEL5');
            END;
         END LOOP;

         COMMIT;
      END;

      -- Moved emp_data_transfer record insert to post-interface from post-script. 5/12/06
      BEGIN
         INSERT INTO EMP_DATA_TRANSFER_LOG(HUB_RUN_DT, START_DT, END_DT)
              VALUES (TRUNC(SYSDATE), G_RUN_START, G_RUN_END);

         COMMIT;
      END;

      HUB_LIB.SET_RUN_DATES(GC_INTERFACE_NAME, G_RUN_START, G_RUN_END);
   END POST_INTERFACE;
   
  PROCEDURE EMERGENCY_NUMBERS 
  IS
    c_this_proc   CONSTANT VARCHAR2(20) := 'EMERGENCY_NUMBERS';
    l_phase                VARCHAR2(100) := 'Initialising';
    
    CURSOR HR_CURSOR
      IS
         SELECT *
           FROM EMPLOYEE
          WHERE HUB_LIB.hrm_convert_to_date (tp, tp_time) >= G_RUN_START2
            AND HUB_LIB.hrm_convert_to_date (tp, tp_time) <= G_RUN_END2;
            
  BEGIN
    FOR HR_REC IN HR_CURSOR
    LOOP
      DECLARE
        var_key_data VARCHAR2(1000);
        var_username VARCHAR2(100);
        var_status   VARCHAR2(10);
      BEGIN
        var_key_data := 'Employee: '||HR_REC.employee#;
        l_phase := 'QV_CLIENT_ROLE lookup';
        
        SELECT username
          INTO var_username
          FROM QV_CLIENT_ROLE
         WHERE role_cd = 'EMP'
           AND id = to_number(HR_REC.employee#);
           
        BEGIN
          l_phase := 'QV_PERS_DETAILS lookup';
          SELECT 'UPDATE'
            INTO var_status
            FROM QV_PERS_DETAILS  qpd
           WHERE qpd.username = var_username;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            var_status := 'CREATE';
        END;
        
        IF var_status = 'CREATE' THEN
          l_phase := 'Create';
          insert into QV_PERS_DETAILS
                            (username
                            ,role_cd  
                            ,qv_update_on  
                            ,emergency_mobile
                            )
                     values (var_username
                            ,'EMP'
                            ,sysdate
                            ,HR_REC.other_phone#
                            );                          
          HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, c_this_proc, 'DEBUG', 'QV_PERS_DETAILS', var_key_data, 'Inserted emergency mobile: '||hr_rec.other_phone#);
             
        ELSE
          l_phase := 'Update';
          update QV_PERS_DETAILS
             set emergency_mobile = hr_rec.other_phone#,
                 qv_update_on = sysdate
           where username = var_username
             and role_cd = 'EMP';
          HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, c_this_proc, 'DEBUG', 'QV_PERS_DETAILS', var_key_data,  'Updated emergency mobile: '||hr_rec.other_phone#);
        END IF;
        
        l_phase := 'Commit';
        commit;
           
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                 c_this_proc,
                                 'ERROR',
                                 'QV_CLIENT_ROLE',
                                 var_key_data,
                                 'No Data Found exception during phase: '||l_phase||', error: '||SQLERRM);
          G_MOVE_DATE_WINDOW_2 := false;
          rollback;
        WHEN OTHERS THEN
          HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                 c_this_proc,
                                 'ERROR',
                                 NULL,
                                 var_key_data,
                                 'When Others exception during phase: '||l_phase||', error: '||SQLERRM);
          G_MOVE_DATE_WINDOW_2 := false;
          rollback;
      END;
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                 c_this_proc,
                                 'ERROR',
                                 NULL,
                                 NULL,
                                 'WHEN OTHERS EXCEPTION: '||SQLERRM);
      G_MOVE_DATE_WINDOW_2 := false;
  END;


   PROCEDURE MAIN_CONTROL
   IS
      C_THIS_PROC   CONSTANT VARCHAR2(20) := 'MAIN_CONTROL';
      L_PHASE                VARCHAR2(50) := 'Initialising';
      L_START_TIME           TIMESTAMP;
      L_END_TIME             TIMESTAMP;
      L_ELAPSED_TIME         INTERVAL DAY(2) TO SECOND(6);
      L_COUNT                NUMBER;
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

      -- SQL Select
      -- IF record does not exist
      SELECT COUNT(*)
        INTO L_COUNT
        FROM EMP_DATA_TRANSFER_LOG
       WHERE QV_RUN_DT IS NULL;

      IF L_COUNT = 0
      THEN
         BEGIN
            L_PHASE := 'EXECUTE PRE_INTERFACE';
            PRE_INTERFACE;
            L_PHASE := 'EXECUTE ORG_CLEVEL1';
            ORG_CLEVEL1;
            L_PHASE := 'EXECUTE ORG_CLEVEL23';
            ORG_CLEVEL23;
            L_PHASE := 'EXECUTE ORG_CLEVEL45';
            ORG_CLEVEL45;
            L_PHASE := 'EXECUTE EMP_CLEVEL5';
            EMP_CLEVEL5;
            L_PHASE := 'EXECUTE EMP_CLEVEL4';
            EMP_CLEVEL4;
            L_PHASE := 'EXECUTE EMP_CLEVEL3';
            EMP_CLEVEL3;
            L_PHASE := 'EXECUTE EMP_CLEVEL2';
            EMP_CLEVEL2;
            L_PHASE := 'EXECUTE EMP_CLEVEL1';
            EMP_CLEVEL1;
            L_PHASE := 'EXECUTE EMP_EMPLOYEE';
            EMP_EMPLOYEE;
            L_PHASE := 'EXECUTE EMP_COUNTRY';
            EMP_COUNTRY;
            L_PHASE := 'EXECUTE EMP_ADDRESS';
            EMP_ADDRESS;
            L_PHASE := 'EXECUTE EMP_LANGUAGE';
            EMP_LANGUAGE;
            L_PHASE := 'EXECUTE EMP_EQUITY';
            EMP_EQUITY;
            L_PHASE := 'EXECUTE EMP_POSITION';
            EMP_POSITION;
            L_PHASE := 'EXECUTE EMP_SUBSTANTIVE';
            EMP_SUBSTANTIVE;
            L_PHASE := 'EXECUTE EMP_CONCURRENT';
            EMP_CONCURRENT;
            L_PHASE := 'EXECUTE EMP_HDA';
            EMP_HDA;
            L_PHASE := 'EXECUTE EMP_SUBSTANTIVE_DEL';
            EMP_SUBSTANTIVE_DEL;
            L_PHASE := 'EXECUTE EMP_CONCURRENT_DEL';
            EMP_CONCURRENT_DEL;
            L_PHASE := 'EXECUTE EMP_HDA_DEL';
            EMP_HDA_DEL;
            L_PHASE := 'EXECUTE POST_INTERFACE';
            POST_INTERFACE;
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
      ELSE
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, C_THIS_PROC, 'ERROR', 'EMP_DATA_TRANSFER_LOG', NULL, 'QV EMP_EMPLOYEE_JOB_TRANSFER DID NOT FINISH SUCCESSFULLY');
      END IF;

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
   
  PROCEDURE MAIN_CONTROL2
  IS
    c_this_proc   CONSTANT VARCHAR2(20) := 'MAIN_CONTROL2';
    l_phase                VARCHAR2(50) := 'Initialising';
    l_start_time           TIMESTAMP;
    l_end_time             TIMESTAMP;
    l_elapsed_time         INTERVAL DAY(2) TO SECOND(6);
  BEGIN
    -- Log that this interface has started.
    l_start_time := localtimestamp;
    HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                      c_this_proc,
                      'INFO',
                      NULL,
                      'Starting '
                      || GC_INTERFACE_NAME||'.'||c_this_proc,
                      'Start at: '
                      || TO_CHAR(l_start_time));
                      
    G_MOVE_DATE_WINDOW_2 := true;
                      
    BEGIN
      HUB_LIB.GET_RUN_DATES(GC_INTERFACE_NAME||'.'||c_this_proc, G_RUN_START2, G_RUN_END2);
      
      OFFSET_RUN_TS(G_RUN_START2);
      l_phase := 'Execute EMERGENCY_NUMBERS';
      EMERGENCY_NUMBERS;      
      
      IF G_MOVE_DATE_WINDOW_2 THEN
          -- Set Run Dates for this scenario
          HUB_LIB.SET_RUN_DATES(GC_INTERFACE_NAME||'.'||c_this_proc, G_RUN_START2, G_RUN_END2);
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                                 c_this_proc,
                                 'ERROR',
                                 NULL,
                                 'MAIN_CONTROL2 When Others exception during Phase: '|| l_phase,
                                 SQLERRM);
    END;  
                      
    -- Log that this interface has finished.
    l_end_time := LOCALTIMESTAMP;
    l_elapsed_time := L_end_time - l_start_time;
    HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                      c_this_proc,
                      'INFO',
                      NULL,
                      'Elapsed time '
                      || l_elapsed_time,
                      'Ended at: '
                      || TO_CHAR(l_end_time));
  END;
END HR_QV;
/
