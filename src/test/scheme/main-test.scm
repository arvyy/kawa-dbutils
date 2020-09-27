(import (scheme base)
        (scheme write)
        (arvyy dbutils)
        (srfi 1)
        (srfi 64))

(define qr (make-query-runner "org.sqlite.JDBC" "jdbc:sqlite:target/testdb" "" ""))

(define-syntax reset-db!
  (syntax-rules ()
    ((_)
     (define _ 
       (let ()
        (update qr "drop table if exists TestTable")
        (update qr "create table TestTable(id integer primary key, val text)")
        (update qr "insert into TestTable(id, val) values (1, 'Foo'), (2, 'Bar')"))))))

(test-begin "Kawa-Dbutils test")

;; on test end exit with non-zero status if there were failures
(let* ((runner (test-runner-current))
       (callback (test-runner-on-final runner)))
  (test-runner-on-final!
    runner
    (lambda (r)
      (callback r)
      (exit (= 0 (test-runner-fail-count r))))))

(test-group
  "Test select many simple"
  (reset-db!)
  
  (define rez (query qr "select * from TestTable" 'M))
  
  (test-equal 2 (length rez))
  (test-assert
    (every 
      (lambda (row)
        (and (assoc "id" row)
             (assoc "val" row)))
      rez)))

(test-group
  "Test select many with params"
  
  (define rez (query qr "select * from TestTable where id = ? and val = ?" 'M 
                      1 "Foo"))
  
  (test-equal
    '((("id" . 1)
       ("val" . "Foo")))
    rez))

(test-group
  "Test select single simple"
  (reset-db!)
  (define rez (query qr "select * from TestTable where id = 1" 'S))
  
  (test-equal
    '("id" . 1)
    (assoc "id" rez))
  
  (test-equal
    '("val" . "Foo")
    (assoc "val" rez)))

(test-group
  "Test select single with params"
  (reset-db!)
  (define rez (query qr "select * from TestTable where id = ?" 'S 1))
  
  (test-equal
    '("id" . 1)
    (assoc "id" rez))
  
  (test-equal
    '("val" . "Foo")
    (assoc "val" rez)))

(test-group
  "Test select single when not exists"
  (reset-db!)
  (define rez (query qr "select * from TestTable where id = ?" 'S 4))
  (test-equal
    #f
    rez))

(test-group
  "Test insert"
  (reset-db!)
  (define count (update qr "insert into TestTable(id, val) values (3, 'Baz')"))
  (test-equal 1 count)
  (let ((rez (query qr "select * from TestTable where id = 3" 'S)))
   (test-equal 
     '("id" . 3)
     (assoc "id" rez))))

(test-group
  "Test insert null"
  (reset-db!)
  (define count (update qr "insert into TestTable(id, val) values (?, ?)" 3 #f))
  (test-equal 1 count)
  (let ((rez (query qr "select * from TestTable where val is null" 'S)))
   (test-equal 
     '("id" . 3)
     (assoc "id" rez))
   (test-equal
     '("val" . #f)
     (assoc "val" rez))))

(test-group
  "Test insert and get autoincrement id"
  (update qr "drop table if exists TestTable")
  (update qr "create table TestTable(id integer primary key autoincrement, val text)")
  (define rez (insert qr "insert into TestTable(val) values('Foo')"))
  (test-equal
    1
    rez))

(test-group
  "Test update"
  (reset-db!)
  (define count (update qr "update TestTable set val = 'Baz' where id = 1"))
  (test-equal 1 count)
  (let ((rez (query qr "select * from TestTable where id = 1" 'S)))
   (test-equal 
     '("val" . "Baz")
     (assoc "val" rez))))

(test-group
  "Test delete"
  (reset-db!)
  (define count (update qr "delete from TestTable where id = 1"))
  (test-equal 1 count)
  (let ((rez (query qr "select * from TestTable where id = 1" 'M)))
   (test-equal 
     '()
     rez)))

(test-group
  "Test transaction commit"
  (reset-db!)
  (call-in-transaction 
    qr
    #f
    (lambda ()
      (update qr "delete from TestTable")
      (test-equal
        '()
        (query qr "select * from TestTable" 'M))
      (commit)))
  (test-equal
    0
    (length (query qr "select * from TestTable" 'M))))

(test-group
  "Test transaction rollback"
  (reset-db!)
  (call-in-transaction 
    qr
    #f
    (lambda ()
      (update qr "delete from TestTable")
      (test-equal
        '()
        (query qr "select * from TestTable" 'M))
      (rollback)))
  (test-equal
    2
    (length (query qr "select * from TestTable" 'M))))

(test-group
  "Test transaction rollback on error"
  (reset-db!)
  (call-in-transaction
    qr
    #f
    (lambda ()
      (update qr "delete from TestTable")
      (call/cc
        (lambda (k)
          (with-exception-handler
            (lambda (err) (k #t))
            (lambda ()
              (call-in-transaction
                qr
                #t
                (lambda ()
                  (test-equal
                    0
                    (length (query qr "select * from TestTable" 'M)))
                  (error "Error inside uncommited transaction")))))))
      (commit)))

  (test-equal
    2
    (length (query qr "select * from TestTable" 'M))))

(test-end)
