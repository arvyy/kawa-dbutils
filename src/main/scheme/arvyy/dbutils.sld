(define-library
  (arvyy dbutils)
  (import
    (scheme base)
    (scheme write)
    (srfi 1)
    (class java.sql Connection)
    (class java.sql ResultSet)
    (class org.apache.commons.dbutils QueryRunner ResultSetHandler)
    (class org.apache.commons.dbcp2 BasicDataSource)
    (kawa lib reflection))
  
  (export
    dbutils-error?
    dbutils-error-message
    dbutils-error-statement
    handler/scalar
    handler/single-of
    handler/list-of
    handler/generator-of
    row-handler/rec
    row-handler/vector
    row-handler/alist
    make-query-runner
    query
    update
    insert
    call-in-transaction
    in-transaction?
    commit
    rollback)
  
  (begin
    
    (define-record-type 
      <dbutils-error>
      (make-dbutils-error message statement)
      dbutils-error?
      (message dbutils-error-message)
      (statement dbutils-error-statement))
    
    (define (exception-handler/attach-statement statement)
      (lambda (err)
        (define new-err 
          (if (dbutils-error? err)
              (make-dbutils-error (dbutils-error-message err) statement)
              (make-dbutils-error ((->java.lang.Throwable err):getMessage) statement)))
        (raise new-err)))
    
    (define (f->null el)
      (if el el #!null))
    
    (define (null->f el)
      (if (equal? el #!null) #f el))
    
    ;; active connection when in transaction
    (define connection (make-parameter #!null))
    
    (define (in-transaction?)
      (if (connection) #t #f))
    
    (define (call-in-transaction query-runner use-existing? thunk)
      (define curr-connection (connection))
      (define qr (->QueryRunner query-runner))
      (define (thunk*)
        (with-exception-handler
          (lambda (k)
            (rollback))
          thunk))
      (cond 
        ((and use-existing? curr-connection)
         (dynamic-wind
           (lambda () #t)
           thunk*
           (lambda () #t)))
        (else 
          (let ((conn ((qr:getDataSource):getConnection)))
           (parameterize 
             ((connection conn))
             (dynamic-wind
               (lambda () (conn:setAutoCommit #f))
               thunk*
               (lambda () (conn:close))))))))
    
    (define (commit)
      (define conn (connection))
      (unless conn (error "Cannot commit -- not in transaction."))
      ((->Connection conn):commit))
    
    (define (rollback)
      (define conn (connection))
      (unless conn (error "Cannot rollback -- not in transaction."))
      ((->Connection conn):rollback))
    
    (define (make-data-source driver ::String  url ::String  name ::String password ::String) ::BasicDataSource
      (define ds (BasicDataSource))
      (ds:setDriverClassName driver)
      (ds:setUrl url)
      (ds:setUsername name)
      (ds:setPassword password)
      ds)
    
    (define (make-query-runner driver ::String  url ::String  name ::String password ::String) ::QueryRunner
      (QueryRunner (make-data-source driver url name password)))
    
    ;; unfortunately it seems syntax instead of proc is needed
    ;; to properly pass IN parameters to varargs parameter on java side
    ;; trying to create and use array for some reason not work
    (define-syntax query
      (syntax-rules ()
        ((query query-runner sql handler arg ...)
         (with-exception-handler
           (exception-handler/attach-statement sql)
           (lambda ()
             (let ()
              (define h ::ResultSetHandler handler)
              (define qr ::QueryRunner query-runner)
              (if (in-transaction?)
                  (let ((conn ::Connection (connection)))
                   (qr:query conn (->string sql) h (f->null arg) ...))
                  (qr:query (->String sql) h (f->null arg) ...))))))))
    
    (define-syntax update
      (syntax-rules ()
        ((update query-runner sql arg ...)
         (with-exception-handler
           (exception-handler/attach-statement sql)
           (lambda ()
             (let ((qr ::QueryRunner query-runner))
              (if (in-transaction?)
                  (let ((conn ::Connection (connection)))
                   (qr:update conn (->String sql) (f->null arg) ...) )
                  (qr:update (->String sql) (f->null arg) ...))))))))
    
    (define-syntax insert
      (syntax-rules ()
        ((insert query-runner sql arg ...)
         (with-exception-handler
           (exception-handler/attach-statement sql)
           (lambda ()
             (let ((qr ::QueryRunner query-runner)
                   (handler ::ResultSetHandler (handler/scalar)))
               (define rez
                 (if (in-transaction?)
                     (let ((conn ::Connection (connection)))
                      (qr:insert conn (->String sql) handler (f->null arg) ...) )
                     (qr:insert (->String sql) handler (f->null arg) ...)))
               rez))))))
    
    (define (handler/scalar) ::ResultSetHandler
      (lambda (rs ::ResultSet)
        (if (not (rs:next))
            (raise (make-dbutils-error "Query returned no results" #f))
            (let ()
              (define rez (null->f (rs:getObject 1)))
              (when (rs:next)
                (raise (make-dbutils-error "Query returned more than one result" #f)))
              rez))))
    
    (define (handler/single-of row-handler) ::ResultSetHandler
      (lambda (rs ::ResultSet)
        (if (rs:next)
            (let ((value (row-handler rs)))
             (when (rs:next)
               (raise (make-dbutils-error "Query returned more than one result" #f)))
             value)
            (raise (make-dbutils-error "Query returned no results" #f)))))
    
    (define (handler/generator-of row-handler consumer) ::ResultSetHandler
      (lambda (rs ::ResultSet)
        (define empty #f)
        (define (generator)
          (cond
            (empty (eof-object))
            ((rs:next) (row-handler rs))
            (else (begin
                    (set! empty #t)
                    (eof-object)))))
        (consumer generator)))

    (define (handler/list-of row-handler) ::ResultSetHandler
      (lambda (rs ::ResultSet)
        (define lst '())
        (define last #f)
        (let loop ((has-next? (rs:next)))
         (if has-next?
             (let ((new-entry (row-handler rs)))
              (if (not last)
                  (begin
                    (set! last (cons new-entry '()))
                    (set! lst last))
                  ;;else has last
                  (let ((new-last (cons new-entry '())))
                   (set-cdr! last new-last)
                   (set! last new-last)))
              (loop (rs:next)))
             ;;else doesn't have next
             lst))))
    
    ;; in kawa, rtd is java.lang.Class 
    (define (row-handler/rec rtd ::java.lang.Class)
      (define fields (record-type-field-names rtd))
      (define constructor (record-constructor rtd))
      (lambda (rs ::ResultSet)
        (define vals
          (map
            (lambda (field-name)
              (null->f (rs:getObject (symbol->string field-name))))
            fields))
        (apply constructor vals)))

    (define (row-handler/alist)
      (lambda (rs ::ResultSet)
        (define meta (rs:getMetaData))
        (define col-count (meta:getColumnCount))
        (let loop ((alist '())
                   (i 1)) ;;java's resultset uses 1-based indexing
          (if (> i col-count)
              alist
              (let* ((col-name (meta:getColumnName i))
                     (key (string->symbol col-name))
                     (value (null->f (rs:getObject i)))
                     (entry (cons key value)))
               (loop (cons entry alist)
                     (+ i 1)))))))
    
    (define (row-handler/vector)
      (lambda (rs ::ResultSet)
        (define meta (rs:getMetaData))
        (define col-count (meta:getColumnCount))
        (define vec (make-vector col-count))
        (let loop ((i 1))
         (if (> i col-count)
             vec
             (begin
               (vector-set! vec (- i 1) (null->f (rs:getObject i)))
               (loop (+ 1 i)))))))))
