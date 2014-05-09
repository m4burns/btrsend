#! /bin/sh
#|
exec /home/m4burns/bin/racket -u "$0" "$@"
|#
#lang racket

(require aws/keys
         aws/glacier
         db
         srfi/19
         srfi/26
         racket/format
         racket/async-channel
         rackunit
         rackunit/text-ui)

(define/contract (make-gpg-compress-pipe recipient)
  (-> string? (list/c output-port? input-port? procedure?))
  (let ((ps (process (format "gzip -7 | gpg --encrypt --recipient ~a" recipient))))
    (thread (thunk (copy-port (fourth ps) (current-error-port))))
    (list (second ps) (first ps) (fifth ps))))

(define/contract (string->sql-date str)
  (-> string? sql-date?)
  (match str
    [(pregexp #px"^([0-9]{4})-([0-9]{2})-([0-9]{2})$" (list _ y m d))
     (sql-date (string->number y)
               (string->number m)
               (string->number d))]
    [else (error 'string->sql-date "Invalid date string: ~a" str)]))

(define/contract (sql-date->string str)
  (-> sql-date? string?)
  (string-append
    (~a (sql-date-year str)  #:width 4 #:align 'right #:left-pad-string "0") "-"
    (~a (sql-date-month str) #:width 2 #:align 'right #:left-pad-string "0") "-"
    (~a (sql-date-day str)   #:width 2 #:align 'right #:left-pad-string "0")))

(define/contract (sql-date->date19 date)
  (-> sql-date? date?)
  (make-date 0 0 0 0 (sql-date-day date) (sql-date-month date) (sql-date-year date) 0))

(define/contract (date19->sql-date date)
  (-> date? sql-date?)
  (sql-date (date-year date) (date-month date) (date-day date)))

(define/contract (sql-date-subtract-days date days)
  (-> sql-date? exact-nonnegative-integer? sql-date?)
  (date19->sql-date
    (time-utc->date
      (subtract-duration
        (date->time-utc
          (sql-date->date19 date))
        (make-time time-duration 0 (* 86400 days)))
      0)))

(define/contract (sql-date<= date1 date2)
  (-> sql-date? sql-date? boolean?)
  (time<=? (date->time-utc (sql-date->date19 date1))
           (date->time-utc (sql-date->date19 date2))))

(define/contract (byte-count->human-readable byte-count)
  (-> exact-nonnegative-integer? string?)
  (define (fmt num q) (format "~a ~a" (~r num #:precision 2 #:min-width 6) q))
  (match (quotient (sub1 (integer-length byte-count)) 10)
    [0 (fmt byte-count " B")]
    [1 (fmt (/ byte-count (expt 2 10)) "KB")]
    [2 (fmt (/ byte-count (expt 2 20)) "MB")]
    [3 (fmt (/ byte-count (expt 2 30)) "GB")]
    [4 (fmt (/ byte-count (expt 2 40)) "TB")]
    [x (fmt (/ byte-count (expt 2 50)) "PB")]))

(define (query-rows-hash db query-str . args)
  (define rows (apply query `(,db ,query-str ,@args)))
  (define keys (map (compose string->symbol cdr (cut assoc 'name <>))
                    (rows-result-headers rows)))
  (map
    (lambda(row)
      (for/hash ([k keys] [v row])
        (values k v)))
    (rows-result-rows rows)))

(define/contract (make-initialized-db filename)
  (-> path-string? connection?)
  (define db-create-stmt
    "CREATE TABLE btrsend \
      ( id INTEGER PRIMARY KEY AUTOINCREMENT,
        arn VARCHAR(1000) NOT NULL, \
        date DATETIME NOT NULL, \
        size INTEGER NOT NULL, \
        parent DATE )")
  (define db (sqlite3-connect #:database filename
                              #:mode     'create))
  (unless
    (query-maybe-value db "SELECT name FROM sqlite_master WHERE type='table' AND name='btrsend'")
    (query-exec db db-create-stmt))
  db)

(define (list-backups db)
  (define (make-key-date hs k)
    (map
      (lambda(h)
        (if (string? (hash-ref h k #f))
            (hash-update h k (cut string->sql-date <>))
            h))
      hs))
  (make-key-date
    (make-key-date
      (query-rows-hash db "SELECT * FROM btrsend ORDER BY date(date) ASC")
      'date)
    'parent))

(define/contract (delete-backups-before db vault date really?)
  (-> connection? string? sql-date? boolean?
      (values exact-nonnegative-integer? exact-nonnegative-integer?))
  (for/fold ([num-deleted 0]
             [bytes-deleted 0])
            ([(id arn size)
              (in-query db "SELECT id,arn,size FROM btrsend WHERE date <= ?"
                        (sql-date->string date))])
    (if (or (not really?)
            (delete-archive vault arn))
        (begin
          (query-exec db "DELETE FROM btrsend WHERE id = ?" id)
          (values (add1 num-deleted) (+ bytes-deleted size)))
        (begin
          (fprintf (current-error-port)
                   "Warning: Couldn't delete backup for date ~a from Glacier (arn = ~a)~n"
                   (sql-date->string date)
                   arn)
          (values num-deleted bytes-deleted)))))

(define/contract (create-backup db vault date parent input-port really?)
  (-> connection? string? sql-date? (or/c sql-date? #f) input-port? boolean? void?)
  (fprintf (current-error-port)
           "Attempting to create backup for date ~a with parent ~a~n"
           (sql-date->string date)
           (if parent
               (sql-date->string parent)
               "none"))
  (define/contract (dummy-archive-sink vault pipe-in desc #:part-size [part-size (* 1024 1024)])
    (-> string? input-port? string? #:part-size exact-nonnegative-integer? string?)
    (call-with-output-file "/dev/null"
      (cut copy-port pipe-in <>)
      #:exists 'append)
    "fake-arn")
  (define bfr-size  (* 16 1024 1024))
  (define part-size (* 32 1024 1024))
  (define-values (pipe-in pipe-out) (make-pipe (* 8 part-size)))
  (define status-chan (make-async-channel))
  (define final-size-chan (make-async-channel))
  (define arn-chan (make-async-channel))
  (define courier
    (thread
      (thunk
        (async-channel-put
          final-size-chan
          (let ([buffer (make-bytes bfr-size)])
            (let loop ([bytes-written 0])
              (async-channel-put status-chan bytes-written)
              (let ([res (read-bytes! buffer input-port)])
                (if (eof-object? res)
                    bytes-written
                    (loop (+ bytes-written (write-bytes buffer pipe-out 0 res)))))))))))
  (define uploader
    (thread
      (thunk
        (async-channel-put
          arn-chan
          ((if really?
               create-archive-from-port
               dummy-archive-sink)
            vault
            pipe-in
            (format "btrsend backup for ~a ~a"
                    (sql-date->string date)
                    (if parent
                      (format "(parent = ~a)" (sql-date->string parent))
                      "(complete)"))
            #:part-size part-size)))))

  (define start-time (current-inexact-milliseconds))
  (let loop
    ([progress (sync status-chan courier)])
    (if (thread? progress)
        (void)
        (begin
          (fprintf (current-error-port)
                   "Transferred ~a (~a/sec)~n"
                   (byte-count->human-readable progress)
                   (byte-count->human-readable
                     (inexact->exact
                       (round (/ progress
                                 (- (current-inexact-milliseconds) start-time -1)
                                 1/1000)))))
          (loop (sync status-chan courier)))))

  (close-output-port pipe-out)
  (thread-wait uploader)

  (define backup-size (async-channel-get final-size-chan))
  (define backup-arn  (async-channel-get arn-chan))

  (if backup-arn
      (begin
        (fprintf (current-error-port)
                 "Successfully created backup, arn = ~a~n" backup-arn)
        (query-exec db "INSERT INTO btrsend (arn, date, size, parent) VALUES (?, ?, ?, ?)"
                    backup-arn
                    (sql-date->string date)
                    backup-size
                    (if parent
                        (sql-date->string parent)
                        sql-null)))
      (fprintf (current-error-port)
               "Error: Glacier archive creation failed~n"))
  (void))

(define/contract (backup-planner db current-date)
  (-> connection? sql-date? (values (or/c sql-date? #f)
                                    (or/c sql-date? 'complete #f)))
  (define backup-days '(1 7 14 21))
  (define backup-tree-lifetime-days 90)
  (define backup-tree-max-height 11)
  (define backups (list-backups db))
  (define earliest-complete-backup 
    (findf (lambda(x) (not (sql-null->false (hash-ref x 'parent)))) backups))
  (define latest-complete-backup 
    (findf (lambda(x) (not (sql-null->false (hash-ref x 'parent)))) (reverse backups)))
  (define (children-of row)
    (reverse
      (for/fold
        ([result (list row)])
        ([backup backups])
        (if (member (hash-ref backup 'parent) (map (cut hash-ref <> 'date) result))
            (cons backup result)
            result))))

  (define create?
    (or (empty? backups)
        (and (member (sql-date-day current-date) backup-days)
             (not (findf (lambda(x) (equal? current-date (hash-ref x 'date))) backups)))))
  (define delete-before
    (if earliest-complete-backup
        (let ((earliest-ch (children-of earliest-complete-backup)))
          (if (and (not (empty? earliest-ch))
                   (sql-date<= (hash-ref (last earliest-ch) 'date)
                               (sql-date-subtract-days
                                 current-date
                                 backup-tree-lifetime-days)))
              (hash-ref (last earliest-ch) 'date)
              #f))
        #f))
  (define parent
    (if latest-complete-backup
        (let ((latest-ch (children-of latest-complete-backup)))
          (if (<= 1 (length latest-ch) backup-tree-max-height)
              (hash-ref (last latest-ch) 'date)
              #f))
        #f))

  (values
    delete-before
    (cond
      [(and create? parent) parent]
      [create? 'complete]
      [else #f])))

(define (btrsend backing-up-date-str data-input-port req-output-port)
  (define vault "Backup")
  (define db (make-initialized-db ".btrsend.db"))
  (define backing-up-date (string->sql-date backing-up-date-str))
  (define (create-encrypted parent)
    (define ps (make-gpg-compress-pipe "m4burns@uwaterloo.ca"))
    (define courier
      (thread
        (thunk
          (copy-port data-input-port (first ps))
          (close-output-port (first ps)))))
    (create-backup db vault backing-up-date parent (second ps) #t)
    (thread-wait courier)
    ((third ps) 'wait)
    (unless (= 0 ((third ps) 'exit-code))
      (fprintf (current-error-port) "Warning: compress/encrypt process exited with non-zero code")))
  (define-values (delete create)
    (backup-planner db backing-up-date))
  (when delete
    (delete-backups-before db vault delete #t))
  (cond
    [(eq? create 'complete)
     (fprintf req-output-port "c~n")
     (flush-output req-output-port)
     (create-encrypted #f)]
    [create
     (fprintf req-output-port "i ~a~n" (sql-date->string create))
     (flush-output req-output-port)
     (create-encrypted create)]
    [else (void)]))

(module+ main
  (define (main . args)
    (unless (= (length args) 1)
      (fprintf (current-error-port) "Usage: btrsend.rkt [backing-up-date]~n")
      (exit 1))

    (define stdin  (current-input-port))
    (define stdout (current-output-port))
    (define stderr (current-error-port))

    (call-with-output-file ".btrsend.log"
      (lambda(log-port)
        (file-stream-buffer-mode log-port 'line)
        (parameterize
          ([current-input-port (input-port-append #f)]
           [current-output-port log-port]
           [current-error-port  log-port])
          (btrsend (first args) stdin stdout)))
      #:exists 'append)

    (exit 0))
  (apply main (vector->list (current-command-line-arguments))))

(module+ test
  (define (make-test-db)
    (when (file-exists? "btrsend-test.db")
      (delete-file "btrsend-test.db"))
    (make-initialized-db "btrsend-test.db"))

  (define-check (check-planner db date expect-delete expect-create)
    (let-values ([(d c) (backup-planner db date)])
      (unless
        (and (equal? d expect-delete)
             (equal? c expect-create))
        (fail-check
          (format "actual-delete: ~a~nactual-create: ~a" d c)))))

  (define-syntax-rule (check-create db date parent size)
    (check-not-exn
      (thunk
        (create-backup db "" date parent (open-input-bytes (make-bytes size 0)) #f))))

  (define-syntax-rule (check-delete db before expect-num expect-bytes)
    (test-begin
      (define num-deleted #f)
      (define bytes-deleted #f)
      (check-not-exn
        (thunk
          (let-values ([(n b) (delete-backups-before db "" before #f)])
            (set! num-deleted n)
            (set! bytes-deleted b))))
      (check-eqv? num-deleted expect-num)
      (check-eqv? bytes-deleted expect-bytes)))

  (define (expected-record id date parent size)
    `#hash((id . ,id)
           (date . ,date)
           (arn . "fake-arn")
           (size . ,size)
           (parent . ,(false->sql-null parent))))

  (define-test-suite helper-tests
    (check-equal? (string->sql-date "2014-01-01") (sql-date 2014 1 1))
    (check-exn exn:fail? (thunk (string->sql-date "asdf")))
    (check-equal? (sql-date->string (sql-date 2014 1 1)) "2014-01-01")
    (check-equal? (sql-date->string (sql-date 1111 11 11)) "1111-11-11")
    (check-equal? (sql-date-subtract-days (sql-date 2014 1 1) 12345)
                  (sql-date 1980 3 15))
    (check-true  (sql-date<= (sql-date 2013 12 12) (sql-date 2014 01 01)))
    (check-false (sql-date<= (sql-date 2014 01 01) (sql-date 1999 12 12)))
    (check-equal? (byte-count->human-readable 1234567) "  1.18 MB")
    (test-begin
      (define ps (make-gpg-compress-pipe "m4burns@uwaterloo.ca"))
      (define th
        (thread
          (thunk
            (call-with-output-file "btrsend-crypttest.gpg"
              (lambda(out)
                (copy-port (second ps) out))
              #:exists 'replace))))
      (write-bytes (make-bytes (* 1024 1024) 65) (first ps))
      (close-output-port (first ps))
      ((third ps) 'wait)
      (check-eqv? ((third ps) 'exit-code) 0)
      (thread-wait th)))

  (define-test-suite logic-tests
    (test-begin
      (define test-db (make-test-db))
      (check-create test-db (sql-date 1234 1 1) #f 8)
      (check-equal?
        (list-backups test-db)
        `(,(expected-record 1 (sql-date 1234 1 1) #f 8)))
      (check-delete test-db (sql-date 2014 1 1) 1 8))

    (test-begin
      (define test-db (make-test-db))
      (check-create test-db (sql-date 1234 1 1) #f (* 320 1024 1024))
      (check-create test-db (sql-date 1234 2 1) (sql-date 1234 1 1) 12345)
      (check-create test-db (sql-date 2014 1 1) (sql-date 1234 2 1) 1)
      (check-equal?
        (list-backups test-db)
        `(,(expected-record 1 (sql-date 1234 1 1) #f (* 320 1024 1024))
           ,(expected-record 2 (sql-date 1234 2 1) (sql-date 1234 1 1) 12345)
           ,(expected-record 3 (sql-date 2014 1 1) (sql-date 1234 2 1) 1)))
      (check-delete test-db (sql-date 2014 1 1) 3 (+ (* 320 1024 1024) 12345 1))
      (check-pred empty? (list-backups test-db)))

    (test-begin
      (define test-db (make-test-db))
      ;; Root 1
      (check-planner test-db (sql-date 2000 1 1) #f 'complete)
      (check-create  test-db (sql-date 2000 1 1) #f 1)
      (check-planner test-db (sql-date 2000 1 1) #f #f)
      (check-planner test-db (sql-date 2000 1 2) #f #f)
      ;; Child of 1
      (check-planner test-db (sql-date 2000 1 7) #f (sql-date 2000 1 1))
      (check-create  test-db (sql-date 2000 1 7) (sql-date 2000 1 1) 1)
      (check-planner test-db (sql-date 2000 1 8) #f #f)
      ;; Children 2 -> 11
      (for ([m  '(1 1 2 2 2 2 3 3 3 3)]
            [pm '(1 1 1 2 2 2 2 3 3 3)]
            [d  (in-cycle '(14 21 1 7))]
            [pd (in-cycle '(7 14 21 1))])
        (check-planner test-db (sql-date 2000 m d) #f (sql-date 2000 pm pd))
        (check-create  test-db (sql-date 2000 m d) (sql-date 2000 pm pd) 1))
      ;; This completes cycle 1
      (check-planner test-db (sql-date 2000 4 1) #f 'complete)
      (check-create  test-db (sql-date 2000 4 1) #f 1)
      ;; Children 1 -> 11
      (for ([m  '(4 4 4 5 5 5 5 6 6 6)]
            [pm '(4 4 4 4 5 5 5 5 6 6)]
            [d  (in-cycle '(7 14 21 1))]
            [pd (in-cycle '(1 7 14 21))])
        (check-planner test-db (sql-date 2000 m d) #f (sql-date 2000 pm pd))
        (check-create  test-db (sql-date 2000 m d) (sql-date 2000 pm pd) 1))
      (check-planner test-db (sql-date 2000 6 21) (sql-date 2000 3 21) (sql-date 2000 6 14))
      (check-create  test-db (sql-date 2000 6 21) (sql-date 2000 6 14) 1)
      (check-delete  test-db (sql-date 2000 3 21) 12 12)
      ;; This completes cycle 2
      (check-planner test-db (sql-date 2000 7 1) #f 'complete)
      (check-create  test-db (sql-date 2000 7 1) #f 1)
      (check-delete  test-db (sql-date 9999 1 1) 13 13)))

  (void (run-tests helper-tests))
  (void (run-tests logic-tests)))
