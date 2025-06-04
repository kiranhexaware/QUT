CREATE OR REPLACE PACKAGE HUB_REPORT
AS
   PROCEDURE HUB_DAILY_REPORT;
   PROCEDURE HUB_ERROR_REPORT;
END HUB_REPORT;
/


CREATE OR REPLACE PACKAGE BODY HUB_REPORT

-- Deployed on: Mon Apr  7 15:18:39 EST 2014
-- Deployed from: intdeploy.qut.edu.au:/home/integsvc/novoP/hub/HUB_REPORT/tags/1.3.2/Packages/HUB_REPORT.pkb

AS
  /*
   25 Jul 2011  DP  Make Hub_Daily_Report insensitive to case differences
                    between JOB name and SCHEDULE name
  */
   GC_INTERFACE_NAME   CONSTANT VARCHAR2(20) := 'HUB_REPORT';

   PROCEDURE HUB_DAILY_REPORT
   IS
      -- Getting all the jobs in the scheduler
      CURSOR REPORT_CURSOR
      IS
         SELECT REPLACE(JOB_NAME, '_JOB', '') AS INTERFACE_NAME,
                REPEAT_INTERVAL, ENABLED
           FROM USER_SCHEDULER_JOBS;

      L_ERRORS               NUMBER;
      L_FROM                 VARCHAR2(100);
      L_TO                   VARCHAR2(100);
      L_BODY                 VARCHAR2(4000) := '';
      L_EOF                  CHAR(1) := CHR(10);
      L_START_TIME           TIMESTAMP;
      L_END_TIME             TIMESTAMP;
      L_ELAPSED_TIME         INTERVAL DAY(2) TO SECOND(6);
      L_RUN_START            TIMESTAMP;
      L_RUN_END              TIMESTAMP;
      L_SUBJECT              VARCHAR2(100);
      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'HUB_DAILY_REPORT';
      VAR_TABLE_NAME         VARCHAR2(4000) := '';
   BEGIN
      BEGIN
         -- Logging the start of this job
         L_START_TIME := LOCALTIMESTAMP;
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                           VAR_TRANSACTION_NAME,
                           'INFO',
                           NULL,
                           'Starting '
                           || GC_INTERFACE_NAME,
                           'Start at: '
                           || TO_CHAR(L_START_TIME));

         -- Email details from control values
         HUB_LIB.GET_HUB_VALUE(GC_INTERFACE_NAME, 'FROM', L_FROM);
         HUB_LIB.GET_HUB_VALUE(GC_INTERFACE_NAME, 'TO', L_TO);
         HUB_LIB.GET_HUB_VALUE(GC_INTERFACE_NAME||'.'||VAR_TRANSACTION_NAME, 'SUBJECT', L_SUBJECT);

         -- Grab the run window from control values table
         HUB_LIB.GET_RUN_DATES(   GC_INTERFACE_NAME
                               || '.'
                               || VAR_TRANSACTION_NAME,
                               L_RUN_START,
                               L_RUN_END);

         -- Preparing the Header Row in the email body
         L_BODY :=
               RPAD('INTERFACE_NAME', 25, ' ')
            || RPAD('ENABLED', 10, ' ')
            || RPAD('ANY ERRORS', 15, ' ')
            || 'REPEAT_INTERVAL'
            || L_EOF;

         -- Start looping through each schedule job
         FOR REPORT_REC IN REPORT_CURSOR
         LOOP
            BEGIN
               -- Set the error value to YES if there are any error for this interface
               SELECT COUNT(*)
                 INTO L_ERRORS
                 FROM HUB_EVENT_LOG
                WHERE EVENT_TYPE = 'ERROR'
                  AND PROC_TIMESTAMP BETWEEN L_RUN_START AND L_RUN_END
                  -- 25 Jul 2011  DP  Make this case-insensitive
                  AND UPPER(INTERFACE_NAME) = UPPER(REPORT_REC.INTERFACE_NAME);
            END;

            -- Setting in the values for this row (interface)

            L_BODY :=
                  L_BODY
               || L_EOF
               || RPAD(REPORT_REC.INTERFACE_NAME, 25, ' ')
               || RPAD(REPORT_REC.ENABLED, 10, ' ')
               || RPAD(TO_CHAR(L_ERRORS, 'B99999'), 15, ' ')
               || REPORT_REC.REPEAT_INTERVAL;
         END LOOP;

         L_SUBJECT :=
               L_SUBJECT
            || '  ['
            || TO_CHAR(L_RUN_START, 'dd-MON-YYYY hh24:mi')
            || ' to '
            || TO_CHAR(L_RUN_END, 'dd-MON-YYYY hh24:mi')
            || ']';
         HUB_LIB.SEND_EMAIL(L_FROM, L_TO, L_SUBJECT, L_BODY);

         -- Move the date window up to current time
         HUB_LIB.SET_RUN_DATES(   GC_INTERFACE_NAME
                               || '.'
                               || VAR_TRANSACTION_NAME,
                               L_RUN_START,
                               L_RUN_END);
      EXCEPTION
         WHEN OTHERS
         THEN
            HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                              VAR_TRANSACTION_NAME,
                              'ERROR',
                              VAR_TABLE_NAME,
                              NULL,
                              'WHEN OTHERS EXCEPTION :'
                              || SQLERRM);
      END;

      -- Logging the end of this job
      L_END_TIME := LOCALTIMESTAMP;
      L_ELAPSED_TIME :=
         L_END_TIME
         - L_START_TIME;
      HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                        VAR_TRANSACTION_NAME,
                        'INFO',
                        NULL,
                        'Elapsed time '
                        || L_ELAPSED_TIME,
                        'Ended at: '
                        || TO_CHAR(L_END_TIME));
   END HUB_DAILY_REPORT;

   PROCEDURE HUB_ERROR_REPORT
   IS
      CURSOR REPORT_CURSOR(P_RUN_START TIMESTAMP,
                           P_RUN_END TIMESTAMP)
      IS
         (  SELECT INTERFACE_NAME, COUNT(*) AS ERROR_TOTAL
              FROM HUB_EVENT_LOG
             WHERE EVENT_TYPE = 'ERROR'
                   AND PROC_TIMESTAMP BETWEEN P_RUN_START AND P_RUN_END
          GROUP BY INTERFACE_NAME);

      L_EVENT_LOG_RT         REPORT_CURSOR%ROWTYPE;
      L_ERRORS               VARCHAR2(4000);
      L_FROM                 VARCHAR2(100);
      L_TO                   VARCHAR2(100);
      L_BODY                 VARCHAR2(4000) := '';
      L_EOF                  CHAR(1) := CHR(10);
      L_START_TIME           TIMESTAMP;
      L_END_TIME             TIMESTAMP;
      L_ELAPSED_TIME         INTERVAL DAY(2) TO SECOND(6);
      L_ERROR_QUERY          VARCHAR2(200);
      L_RUN_START            TIMESTAMP;
      L_RUN_END              TIMESTAMP;
      L_SUBJECT              VARCHAR2(100);

      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'HUB_ERROR_REPORT';
      VAR_TABLE_NAME         VARCHAR2(4000) := '';
   BEGIN
      BEGIN
         -- Logging the start of this job
         L_START_TIME := LOCALTIMESTAMP;
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                           VAR_TRANSACTION_NAME,
                           'INFO',
                           NULL,
                           'Starting '
                           || GC_INTERFACE_NAME,
                           'Start at: '
                           || TO_CHAR(L_START_TIME));


         -- Email details from control values
         HUB_LIB.GET_HUB_VALUE(GC_INTERFACE_NAME, 'FROM', L_FROM);
         HUB_LIB.GET_HUB_VALUE(GC_INTERFACE_NAME, 'TO', L_TO);
         HUB_LIB.GET_HUB_VALUE(GC_INTERFACE_NAME||'.'||VAR_TRANSACTION_NAME, 'SUBJECT', L_SUBJECT);

         HUB_LIB.GET_RUN_DATES(   GC_INTERFACE_NAME
                               || '.'
                               || VAR_TRANSACTION_NAME,
                               L_RUN_START,
                               L_RUN_END);

         -- Preparing Header Row in email body
         L_BODY :=
               RPAD('INTERFACE_NAME', 25, ' ')
            || RPAD('TOTAL_ERRORS', 15, ' ')
            || RPAD('ERROR_QUERY', 15, ' ')
            || L_EOF;

         -- Control variable to decide if email should be sent
         L_ERRORS := 'NO';

         -- Get the errors from the event log table
         OPEN REPORT_CURSOR(L_RUN_START, L_RUN_END);

         LOOP
            FETCH REPORT_CURSOR INTO L_EVENT_LOG_RT;

            EXIT WHEN REPORT_CURSOR%NOTFOUND;

            -- if there are any errors found for this interface, then add the information to the email body
            IF (L_EVENT_LOG_RT.ERROR_TOTAL > 0)
            THEN
               L_ERRORS := 'YES';

               L_ERROR_QUERY :=
                  'SELECT * FROM HUB_EVENT_LOG where event_type=''ERROR'' AND interface_name='''
                  || L_EVENT_LOG_RT.INTERFACE_NAME
                  || ''' and proc_timestamp between '
                  || ''''
                  || L_RUN_START
                  || ''''
                  || ' AND '
                  || ''''
                  || L_RUN_END
                  || ''''
                  || ';';

               L_BODY :=
                     L_BODY
                  || L_EOF
                  || RPAD(L_EVENT_LOG_RT.INTERFACE_NAME, 25, ' ')
                  || RPAD(TO_CHAR(L_EVENT_LOG_RT.ERROR_TOTAL), 15, ' ')
                  || L_ERROR_QUERY;
            END IF;
         END LOOP;

         CLOSE REPORT_CURSOR;

         -- IF error found, then send the email
         IF L_ERRORS = 'YES'
         THEN
            L_SUBJECT :=
                  L_SUBJECT
               || '  ['
               || TO_CHAR(L_RUN_START, 'dd-MON-YYYY hh24:mi')
               || ' to '
               || TO_CHAR(L_RUN_END, 'dd-MON-YYYY hh24:mi')
               || ']';
            HUB_LIB.SEND_EMAIL(L_FROM, L_TO, L_SUBJECT, L_BODY);
         END IF;

         -- Move the date window to current time.
         HUB_LIB.SET_RUN_DATES(   GC_INTERFACE_NAME
                               || '.'
                               || VAR_TRANSACTION_NAME,
                               L_RUN_START,
                               L_RUN_END);
      EXCEPTION
         WHEN OTHERS
         THEN
            IF REPORT_CURSOR%ISOPEN
            THEN
               CLOSE REPORT_CURSOR;
            END IF;

            HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                              VAR_TRANSACTION_NAME,
                              'ERROR',
                              VAR_TABLE_NAME,
                              NULL,
                              'WHEN OTHERS EXCEPTION :'
                              || SQLERRM);
      END;

      -- Log this job has finished.
      L_END_TIME := LOCALTIMESTAMP;
      L_ELAPSED_TIME :=
         L_END_TIME
         - L_START_TIME;
      HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME,
                        VAR_TRANSACTION_NAME,
                        'INFO',
                        NULL,
                        'Elapsed time '
                        || L_ELAPSED_TIME,
                        'Ended at: '
                        || TO_CHAR(L_END_TIME));
   END HUB_ERROR_REPORT;
END HUB_REPORT;
/
