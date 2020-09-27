(define-library
  (arvyy dbutils)
  (import
    (scheme base)
    (scheme write)
    (srfi 1)
    (class java.sql Connection)
    (class org.apache.commons.dbutils QueryRunner ResultSetHandler)
    (class org.apache.commons.dbutils.handlers MapHandler MapListHandler ScalarHandler)
    (class org.apache.commons.dbcp2 BasicDataSource))
  (export
    make-query-runner
    query
    update
    insert
    call-in-transaction
    in-transaction?
    commit
    rollback)
  
  (begin
    
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
    
    (define (map->alist m)
      (define (entry->pair e ::java.util.Map:Entry)
        (cons
          (e:getKey)
          (null->f (e:getValue))))
      (cond
        ((equal? m #!null) #f)
        (else (map entry->pair ((->java.util.Map m):entrySet)))))
    
    (define (maps->alists m)
      (map
        map->alist
        m))
    
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
        ((query query-runner sql mode arg ...)
         (let ()
          (define-values 
            (handler post)
            (cond 
              ((equal? 'S mode) 
               (values (MapHandler) map->alist))
              ((equal? 'M mode)
               (values (MapListHandler) maps->alists))
              (else (error "expected mode to be S (single row result) or M (multiple rows result)"))))
          (define h ::ResultSetHandler handler)
          (define qr ::QueryRunner query-runner)
          (define rez 
            (if (in-transaction?)
                (let ((conn ::Connection (connection)))
                 (qr:query conn (->string sql) h (f->null arg) ...))
                (qr:query (->String sql) h (f->null arg) ...)))
          (post rez)))))
    
    (define-syntax update
      (syntax-rules ()
        ((update query-runner sql arg ...)
         (let ((qr ::QueryRunner query-runner))
          (if (in-transaction?)
              (let ((conn ::Connection (connection)))
               (qr:update conn (->String sql) (f->null arg) ...) )
              (qr:update (->String sql) (f->null arg) ...))))))
    
    (define-syntax insert
      (syntax-rules ()
        ((insert query-runner sql arg ...)
         (let ((qr ::QueryRunner query-runner)
               (handler ::ResultSetHandler (ScalarHandler)))
          (define rez
            (if (in-transaction?)
              (let ((conn ::Connection (connection)))
               (qr:insert conn (->String sql) handler (f->null arg) ...) )
              (qr:insert (->String sql) handler (f->null arg) ...)))
          rez))))))
