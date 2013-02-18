What is this?
-------------

This is a simple python script (using boto and MySQLdb) to automate the profiling of Amazon RDS instances (setup, load, test, teardown), varying the parameter group settings as specified.

Why would I need it?
--------------------

This was written as part of an effort to see how changing MySQL server settings might affect the performance of the database.  Amazon's RDS service was used as it makes it very easy to setup multiple DBMSs in parallel.  Reading all the books and help on optimizing MySQL is useful, but is no substitute for actual tests.

One thing to note is that Amazon might throttle the RDS instances depending on the CPU load, so tests and the analysis of them should take that into account.

How can I use it?
-----------------

The code is only a blueprint - you will need to at least modify the settings at the top and modify the two functions at the top.  Those two functions load the database with test data and also execute the SQL that will be profiled.

Other possible modifications include adding support for Oracle and SQL Server, or support for testing a local DBMS.
