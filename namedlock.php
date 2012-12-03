<?php
// This file is part of Moodle - http://moodle.org/
//
// Moodle is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Moodle is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with Moodle.  If not, see <http://www.gnu.org/licenses/>.

/*
 * This is a named lock class.
 * Atomicity comes from the database update of only a single row.
 *
 *
 * There are Moodle 2.0-based tests built-in at the end of this script. To run the tests 
 * (*NIX only), see the instructions just before the test code.
 */

define('NAMED_LOCK_LOCKED', 1);
define('NAMED_LOCK_UNLOCKED', 2);
define('NAMED_LOCK_STALE', 3);

abstract class named_lock {
    /** @var this lock's name */
    protected $name;

    /**
     * Attempt to get the lock.
     * @param int $maxwait_sec maximum seconds to wait for the lock
     * @param int $maxwait_usec maximum microseconds to wait for the lock
     * @return int one of NAMED_LOCK_LOCKED, NAMED_LOCK_UNLOCKED, NAMED_LOCK_STALE
     */
    public abstract function lock($maxwait_sec=0, $maxwait_usec=0);

    /**
     * Allow lock users to find out if the lock is currently held or not.
     */
    public abstract function status();

    /**
     * Release the lock held by this object.
     */
    public abstract function unlock();

    /*
     * Factory 
     *
     * If both $maxlife_sec and $maxlife_usec are 0, the lock will never be 
     * stale and will never be automatically broken.
     *
     * @param string $name name of this lock (must be known to all code using this /named/ lock)
     * @param int $maxlife_sec number of seconds this lock may be held
     * @param int $maxlife_usec number of microseconds this lock may be held
     */
    public static final function get_lock_manager($name, $maxlife_sec=30, $maxlife_usec=0) {
        global $CFG;
        $class = 'named_lock_db';
        if (isset($CFG->named_lock_manager) and strlen($CFG->named_lock_manager) > 0) {
            $class = 'named_lock_' . $CFG->named_lock_manager;
        }
        return new $class($name, $maxlife_sec, $maxlife_usec);
    }
}

/*
 * Database-based locking class.
 *
 * @package    moodlecore
 * @copyright  2009 Matt Oquist (http://majen.net)
 * @license    http://www.gnu.org/copyleft/gpl.html GNU GPL v3 or later
 */
class named_lock_db extends named_lock {
    /** @var id of this named lock's row in the db */
    private $id;
    /** @var this lock's owning PID */
    protected $pid;
    /** @var when was this lock taken? */
    protected $timestamp;
    /** @var when was this lock taken? (microseconds after $timestamp) */
    protected $utimestamp;
    /** @var a unique ID generated to further distinguish a held lock between systems */
    protected $uniqid;
    /** @var how long the lock may be held in seconds */
    protected $maxlife_sec;
    /** @var how long the lock may be held in microseconds */
    protected $maxlife_usec;

    /** @var state of this lock - not stored in DB, must be one of NAMED_LOCK_LOCKED, NAMED_LOCK_UNLOCKED, NAMED_LOCK_STALE */
    protected $locked;

    /**
     * Contructor - sets object attributes, initializes db if necessary
     *
     * If both $maxlife_sec and $maxlife_usec are 0, the lock will never be 
     * stale and will never be automatically broken.
     *
     * @param string $name name of this lock (must be known to all code using this /named/ lock)
     * @param int $maxlife_sec number of seconds this lock may be held
     * @param int $maxlife_usec number of microseconds this lock may be held 
     */
    function __construct($name, $maxlife_sec=30, $maxlife_usec=0) {
        global $DB;

        if ($maxlife_sec < 0 or $maxlife_usec < 0) {
            throw new Exception("maxlife_sec or maxlife_usec < 0 ($name)");
        }
        if ($maxlife_usec > 1000000) {
            # Seconds belong in $maxlife_sec. $maxlife_usec is only for fractions of a second.
            throw new Exception("maxlife_usec > 1000000 ($name)");
        }

        $this->name = $name;
        $this->pid = getmypid();
        $this->timestamp = null;
        $this->utimestamp = null;
        $this->uniqid = null;
        $this->maxlife_sec = $maxlife_sec;
        $this->maxlife_usec = $maxlife_usec;
        $this->locked = NAMED_LOCK_UNLOCKED;

        if (!$rec = $DB->get_record('named_lock', array('name' => $this->name))) {
            $this->id = $this->init();
        } else {
            $this->id = $rec->id;
        }
    }

    /**
     * Check the db to see if this object currently holds the lock.
     *
     * @return bool true if this object currently holds the lock
     */
    private function checklock() {
        global $DB;
        if ($rec = $DB->get_record('named_lock', array('id' => $this->id, 'pid' => $this->pid, 'timestamp' => $this->timestamp, 'utimestamp' => $this->utimestamp, 'uniqid' => $this->uniqid))) {
            return true;
        }
        return false;
    }

    /**
     * Initialize the db to hold this lock by inserting a row with $this->name 
     * and some other lock attributes.
     * This is only called once per named lock.
     * @return int id of this lock's row in the db
     */
    private function init() {
        global $DB;
        $rec = new object;
        $rec->name = $this->name;
        $rec->maxlife_sec = $this->maxlife_sec;
        $rec->maxlife_usec = $this->maxlife_usec;
        return $DB->insert_record('named_lock', $rec);
    }

    /**
     * Attempt to get the lock.
     * @param int $maxwait_sec maximum seconds to wait for the lock
     * @param int $maxwait_usec maximum microseconds to wait for the lock
     * @return int one of NAMED_LOCK_LOCKED, NAMED_LOCK_UNLOCKED, NAMED_LOCK_STALE
     */
    public function lock($maxwait_sec=0, $maxwait_usec=0) {
        if ($maxwait_sec < 0 or $maxwait_usec < 0) {
            throw new Exception("maxwait_sec or maxwait_usec < 0 ($this->name)");
        }
        if ($maxwait_usec >= 1000000) {
            # $maxwait_usec is only for fractional parts of a second
            throw new Exception("maxwait_usec > 1000000 ($this->name)");
        }
        list($start_usec, $start_sec) = explode(" ", microtime());
        $start_usec *= 1000000;
        $end_sec = $start_sec + $maxwait_sec;
        $end_usec = $start_usec + $maxwait_usec;
        do {
            $this->_lock();
            if ($this->locked === NAMED_LOCK_LOCKED or $this->locked === NAMED_LOCK_STALE) {
                return $this->locked;
            }
            if ($maxwait_sec == 0 and $maxwait_usec == 0) {
                return $this->locked;
            }
            # We need just a bit of indeterminism to avoid deterministic starvation.
            # Sleep for something between .0001 and .02 seconds.
            usleep(rand(100,20000));

            list($now_usec, $now_sec) = explode(" ", microtime());
            $now_usec *= 1000000;
        } while ($now_sec < $end_sec or ($now_sec == $end_sec and $now_usec < $end_usec));
        return $this->locked;
    }

    /**
     * Allow a lock user to find out if she currently holds the lock or not.
     */
    public function status() {
        $this->locked = NAMED_LOCK_UNLOCKED;
        if ($this->checklock()) {
            $this->locked = NAMED_LOCK_LOCKED;
        }
        return $this->locked;
    }

    /**
     * Attempt just one time to take the lock.
     * If the lock is stale, this function breaks the lock in the db and returns
     * NAMED_LOCK_STALE so the external caller may decide what to do next.
     *
     * @return int one of NAMED_LOCK_LOCKED, NAMED_LOCK_UNLOCKED, NAMED_LOCK_STALE
     */
    private function _lock() {
        global $DB, $_SERVER;
        if ($this->locked === NAMED_LOCK_LOCKED) {
            throw new Exception("locking locked lock ($this->name)");
        }

        list($now_usec, $now_sec) = explode(" ", microtime());
        $now_usec *= 1000000;
        $this->timestamp = $now_sec;
        $this->utimestamp = $now_usec;

        # Pass this server's address and a random value in order to further 
        # uniqueify the result. This may matter in a clustering scenario, if two 
        # hosts should happen to attempt to get the lock with identical PIDs at 
        # the same microsecond. It is very unlikely to matter.
        $this->uniqid = uniqid($_SERVER['SERVER_ADDR'].rand());

        $DB->execute("UPDATE {named_lock}
                      SET pid = ?, timestamp = ?, utimestamp = ?, maxlife_sec = ?, maxlife_usec = ?, uniqid = ?
                      WHERE id = ? AND pid IS NULL AND timestamp IS NULL AND utimestamp IS NULL AND uniqid IS NULL",
                      array($this->pid, $this->timestamp, $this->utimestamp, $this->maxlife_sec, $this->maxlife_usec, $this->uniqid, $this->id));
        if ($this->checklock()) {
            $this->locked = NAMED_LOCK_LOCKED;
        } else {
            $this->timestamp = null;
            $this->utimestamp = null;
            $this->uniqid = null;
            list($now_usec, $now_sec) = explode(" ", microtime());
            $now_usec *= 1000000;
            if ($rec = $DB->get_record_select('named_lock', "id = ? AND pid IS NOT NULL AND timestamp IS NOT NULL AND utimestamp IS NOT NULL AND uniqid IS NOT NULL", array('id' => $this->id))) {
                if ($rec->maxlife_sec == 0 and $rec->maxlife_usec == 0) {
                    # This lock is infinitely holdable -- it never goes stale. Evar.
                    $this->locked = NAMED_LOCK_UNLOCKED;
                } else {
                    $end_sec = $rec->timestamp + $rec->maxlife_sec;
                    $end_usec = $rec->utimestamp + $rec->maxlife_usec;
                    if ($now_sec > $end_sec or ($now_sec == $end_sec and $now_usec > $end_usec)) {
                        $this->break_lock($rec);
                    }
                }
            } else {
                # The lock must have been released between our attempt and the 
                # subsequent fetch. If the lock user wants us to try multiple 
                # times, this private method will get called again right away.
                #
                # So don't do anything now.
            }
        }
        return $this->locked;
    }

    /**
     * External caller can break an infinitely-holdable lock.
     * @return int one of NAMED_LOCK_UNLOCKED, NAMED_LOCK_STALE
     */
    public function break_infinite_lock() {
        global $DB;
        if (!$rec = $DB->get_record_select('named_lock', "id = ? AND pid IS NOT NULL AND timestamp IS NOT NULL AND utimestamp IS NOT NULL AND uniqid IS NOT NULL", array('id' => $this->id))) {
            # Great! We don't actually have to break it anymore.
            $this->locked = NAMED_LOCK_UNLOCKED;
        } else {
            if ($rec->maxlife_sec != 0 and $rec->maxlife_usec != 0) {
                # This isn't an infinitely-holdable lock, so this method should not 
                # be used.
                throw new Exception("attempted to forcibly break non-infinite lock ($this->name)");
            }

            # Go ahead. Break it.
            $DB->execute("UPDATE {named_lock}
                          SET pid = NULL, timestamp = NULL, utimestamp = NULL, uniqid = NULL
                          WHERE id = ? AND pid = ? AND timestamp = ? AND utimestamp = ? AND uniqid = ?",
                          array($this->id, $rec->pid, $rec->timestamp, $rec->utimestamp, $rec->uniqid));
            $this->locked = NAMED_LOCK_STALE;
        }

        return $this->locked;
    }

    /**
     * Break a lock that has been held too long.
     * @param object $rec DB record of the lock we will break. We will not break the lock if it has been broken and re-taken already.
     * @return bool success
     */
    private function break_lock($rec) {
        global $DB;

        if ($rec->maxlife_sec === 0 and $rec->maxlife_usec === 0) {
            # This is an infinitely-holdable lock, so this method should not be used.
            # This is a private method, so this must mean there's a bug in this class.
            throw new Exception("attempted to automatically break infinite lock ($this->name)");
        }

        # Break it if the same selfish lock-holder still has it.
        $DB->execute("UPDATE {named_lock}
                      SET pid = NULL, timestamp = NULL, utimestamp = NULL, uniqid = NULL
                      WHERE id = ? AND pid = ? AND timestamp = ? AND utimestamp = ? AND uniqid = ?",
                      array($this->id, $rec->pid, $rec->timestamp, $rec->utimestamp, $rec->uniqid));

        # It's safe to say the lock is stale even if the update did not succeed; 
        # the caller will have to respond as if it's unlocked anyhow.
        $this->locked = NAMED_LOCK_STALE;
        return true;
    }

    /**
     * Release the lock held by this object.
     */
    public function unlock() {
        global $DB;
        if ($this->locked !== NAMED_LOCK_LOCKED) {
            throw new Exception("unlocking obj unlocked lock ($this->name)");
        }
        if ($this->checklock()) {
            $DB->execute("UPDATE {named_lock} SET pid = NULL, timestamp = NULL, utimestamp = NULL, uniqid = NULL WHERE id = ? AND pid = ? AND timestamp = ? AND utimestamp = ? AND uniqid = ?", array($this->id, $this->pid, $this->timestamp, $this->utimestamp, $this->uniqid));
        }
        $this->timestamp = null;
        $this->utimestamp = null;
        $this->uniqid = null;
        $this->locked = NAMED_LOCK_UNLOCKED;
        return $this->locked;
    }
}

/**************************************************************
 * LOCK TESTING
 *
 * The following tests demonstrate ways to use named locks. If desired, these 
 * tests could be commented out but remain in this file if/when this class is 
 * added to Moodle.
 *
 * ############
 * # Note: these tests should use system("flock /tmp/something"), DUH.
 * ############
 * These tests use the atomicity of the *nix 'ln' and 'rm' commands to test the 
 * above locking class. If the lock fails and two locks are granted 
 * simultaneously, there is a high liklihood these tests will catch the problem, 
 * print a scary message, and bail out immediately.
 *
 * These tests use a simple IPC mechanism (the existence of the file /tmp/bail-<lockname>) 
 * to ensure all instances of a given test will exit immediately upon failure so that
 * more data (such as PIDs, timestamps, etc. in use at the time of failure) are 
 * available in the terminals to diagnose problems.
 *
 *
 * TO USE THESE TESTS (*NIX only, sorry):
 *   1) execute the appropriate SQL in your Moodle 2.0 database to create the named_lock table (see below)
 *   2) copy namedlock.php to your Moodle 2.0 dirroot
 *   3) open a second terminal, and in at least two terminals do #4 and #5:
 *   4) cd to your Moodle 2.0 dirroot
 *   5) php ./namedlock.php <parameters>
 *      EXHAUSTIVE LIST OF PARAMETER OPTIONS
 *      $ php ./namedlock.php careful           <task-lifetime-sec> <lock-lifetime-sec> <lock-lifetime-usec>
 *      $ php ./namedlock.php careful-unbail    <task-lifetime-sec> <lock-lifetime-sec> <lock-lifetime-usec>
 *      $ php ./namedlock.php simple            <task-lifetime-sec>
 *      $ php ./namedlock.php simple-break      <task-lifetime-sec>
 *
 *      EXAMPLE 1
 *      # take 25 seconds to do a task while holding a lock with a 30-second lifetime
 *      t1 $ php ./namedlock.php careful 25 30 0
 *      t2 $ php ./namedlock.php careful 25 30 0
 *
 *
 *      EXAMPLE 2
 *      # Take 35 seconds to do a task while holding a lock with a 30-second lifetime - uh-oh!
 *      # The instance in terminal 2 is well-behaved, but will regularly break stale locks held by t1.
 *      t1 $ php ./namedlock.php careful 35 30 0
 *      t2 $ php ./namedlock.php careful 25 30 0
 *
 *
 *      EXAMPLE 3
 *      # take no time to do a task while holding a lock with a .005 lifetime
 *      t1 $ php ./namedlock.php careful 0 0 5000
 *      t2 $ php ./namedlock.php careful 0 0 5000
 *
 *
 *      EXAMPLE 4
 *      # remove the bail-out indicator in t1
 *      t1 $ php ./namedlock.php careful-unbail 0 0 5000
 *      t2 $ php ./namedlock.php careful 0 0 5000
 *
 *
 *      EXAMPLE 5
 *      # demonstrate the simplest-possible lock usage
 *      # Note that if you kill one of the tests while it has the lock, the other test will never get the lock again.
 *      t1 $ php ./namedlock.php simple 15
 *      t2 $ php ./namedlock.php simple 15
 *
 *
 *      EXAMPLE 6
 *      # Demonstrate the simplest-possible lock usage with a forced lock-break in t1.
 *      # The simple lock test uses infinite-lifetime locks, so they are never automatically broken by calling the lock() method.
 *      # Note that if you kill one of the tests while it has the lock, the other test will never get the lock and you'll *need* 
 *      # to run simple-break to get the lock again.
 *      t1 $ php ./namedlock.php simple-break 5
 *      t2 $ php ./namedlock.php simple 5
 *
 *
 *      EXAMPLE 7
 *      # One fast job competing with one slow job.
 *      t1 $ php ./namedlock.php simple-break 1
 *      t2 $ php ./namedlock.php simple 0
 *
 *
 *      EXAMPLE 8
 *      # One fast job competing with one slow job.
 *      t1 $ php ./namedlock.php careful-break 1 1 0
 *      t2 $ php ./namedlock.php careful 0 1 0
 *
 *
 */
if (PHP_SAPI == 'cli') {
    define('CLI_SCRIPT', true);
    $_SERVER['SERVER_ADDR'] = '10.0.0.1'; # this is nonsense but gets rid of warnings
    require_once('./config.php');
    require_once('./lib/dmllib.php');

    # You'll need to create the named_lock table in your DB if it isn't already 
    # there.
    $table_fyi_my = "
        CREATE TABLE `mdl_named_lock` (
            `id` bigint(10) unsigned NOT NULL AUTO_INCREMENT,
            `name` varchar(255) NOT NULL,
            `pid` int,
            `timestamp` int,
            `utimestamp` int,
            `uniqid` varchar(255),
            `maxlife_sec` int,
            `maxlife_usec` int,
            PRIMARY KEY (`id`),
            UNIQUE(name)
        );
        ";

    $table_fyi_pg = "
        CREATE TABLE mdl_named_lock (
            id SERIAL PRIMARY KEY,
            name varchar(255) NOT NULL,
            pid integer,
            timestamp integer,
            utimestamp integer,
            uniqid varchar(255),
            maxlife_sec integer,
            maxlife_usec integer,
            UNIQUE(name)
        );
        ";

    switch ($argv[1]) {
        case 'careful':
            namedlock_test_careful($argv[2], $argv[3], $argv[4], false);
            break;
        case 'careful-unbail':
            namedlock_test_careful($argv[2], $argv[3], $argv[4], true);
            break;
        case 'simple':
            namedlock_test_simple($argv[2], false);
            break;
        case 'simple-break':
            namedlock_test_simple($argv[2], true);
            break;
        default:
            print "Unknown or no test type, using defaults: careful 25 30 0\n";
            namedlock_test_careful(25, 30, 0);
    }
    exit(0);
}

function namedlock_test_careful($tasktime, $locklife_s, $locklife_u, $unbail=false) {
    global $DB;

    if ($locklife_s == 0 and $locklife_u == 0) {
        print "careful test not set up to handle infinite lifetime locks (you must pass a value > 0 in the 2nd or 3rd parameter)\n";
        exit(1);
    }

    $lockname = __FUNCTION__;
    $locktestname = "waka287";
    $rmfirst = false;
    if ($unbail) {
        system("rm -f /tmp/$locktestname");
        system("rm -f /tmp/bail-$lockname");
    }
    $lock = named_lock::get_lock_manager($lockname, $locklife_s, $locklife_u);
    while (true) {
        #$DB->set_debug(true);
        if (file_exists("/tmp/bail-$lockname")) {
            print "$lockname bailing now\n";
            exit(1);
        }

        $wait_sec = $locklife_s;
        $pid = getmypid();
        print "attempting lock at ".time().", will wait $wait_sec seconds...";
        try {
            $l = $lock->lock($wait_sec);
            if ($l === NAMED_LOCK_UNLOCKED) {
                print "no lock after $wait_sec seconds\n";
            } elseif ($l === NAMED_LOCK_STALE) {
                print "stale lock!\n";
                $rmfirst = true;
            } elseif ($l === NAMED_LOCK_LOCKED) {
                print "got lock($pid)...";
                if ($rmfirst) {
                    system("rm -f /tmp/$locktestname", $v);
                    $rmfirst = false;
                }
                system("ln -s /tmp/ /tmp/$locktestname", $v);
                if ($v) {
                    print __FUNCTION__.": LEVEL 1 CRAP\n";
                    system("touch /tmp/bail-$lockname");
                    exit(1);
                }
                sleep($tasktime);
                if ($lock->status() === NAMED_LOCK_LOCKED) {
                    system("rm /tmp/$locktestname", $v);
                    if ($v) {
                        print __FUNCTION__.": LEVEL 2 CRAP\n";
                        system("touch /tmp/bail-$lockname");
                        exit(1);
                    }
                    if ($lock->unlock() === NAMED_LOCK_UNLOCKED) {
                        print "unlocked ($pid)\n";
                    } else {
                        print "unlocking error!\n";
                    }
                } else {
                    print "my lock got broken...\n";
                }
            }
        } catch (Exception $e) {
            system("touch /tmp/bail-$lockname");
            error_log("named_lock exception (".$e->getMessage().")");
            exit(1);
        }
    }
}

function namedlock_test_simple($tasktime, $forced_break=false) {
    global $DB;
    $lockname = __FUNCTION__;
    $locktestname = "waka287";
    $lock = named_lock::get_lock_manager($lockname, 0, 0);
    if ($forced_break) {
        $lock->break_infinite_lock();
        system("rm -f /tmp/$locktestname");
        system("rm -f /tmp/bail-$lockname");
    }
    while (true) {
        #$DB->set_debug(true);
        if (file_exists("/tmp/bail-$lockname")) {
            print "$lockname bailing now\n";
            exit(1);
        }
        $wait_sec = $tasktime;
        $pid = getmypid();
        print "attempting lock at ".time().", will wait $wait_sec seconds...";
        try {
            $l = $lock->lock($wait_sec);
            if ($l !== NAMED_LOCK_LOCKED) {
                print "no lock after $wait_sec seconds\n";
            } else {
                print "got lock($pid)...";
                system("ln -s /tmp/ /tmp/$locktestname", $v);
                if ($v) {
                    print __FUNCTION__.": LEVEL 1 CRAP\n";
                    system("touch /tmp/bail-$lockname");
                    exit(1);
                }
                sleep($tasktime);
                system("rm /tmp/$locktestname", $v);
                if ($v) {
                    print __FUNCTION__.": LEVEL 2 CRAP\n";
                    system("touch /tmp/bail-$lockname");
                    exit(1);
                }
                if ($lock->unlock() === NAMED_LOCK_UNLOCKED) {
                    print "unlocked ($pid)\n";
                } else {
                    print "unlocking error!\n";
                    print __FUNCTION__.": LEVEL 3 CRAP\n";
                    system("touch /tmp/bail-$lockname");
                    exit(1);
                }
            }
        } catch (Exception $e) {
            system("touch /tmp/bail-$lockname");
            error_log("named_lock exception (".$e->getMessage().")");
            exit(1);
        }
    }
}

?>
