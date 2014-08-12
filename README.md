## btrfs snapshot and backup tools ##

Disclaimer: I wrote these tools for my own personal use. They may be insecure
and full of horrible bugs. That said, they work fine for me.

The scripts `btrsnap` and `btrsend` make some assumptions about your filesystem
layout. See `btrsnap` for details. These scripts are provided mostly as
working examples for someone trying to use `btrsend.rkt`. You may wish to
manage your snapshots differently; `btrsend.rkt` does not really care how you
manage your snapshots.

In my setup, `btrsnap` is invoked nightly from root's crontab.

WARNING: The manner in which `btrsend` calls a user's `~/.btrsend` is probably
insecure.

### btrsend.rkt ###

This is the interesting part. This script manages the storage of complete and
incremental backups on Amazon Glacier ( http://aws.amazon.com/glacier/ ). When
it is invoked with the date for which a backup is to be made, it will either
output nothing, the ASCII string `c` followed by a newline, or an ASCII string
of the form `i YYYY-MM-DD` followed by a newline, where YYYY, MM, and DD form a
date.

When nothing is output, this indicates that no backup is to be made for this
date according to the schedule. The program exits immediately.

When `c` is output, a complete serialization of the snapshot for the provided
date is expected on stdin.

When `i YYYY-MM-DD` is output, an incremental serialization of the snapshot
from YYYY-MM-DD to the snapshot for the provided date is expected.

Take care to map dates to snapshots in a consistent way. If, for example, you
were to tell `btrsend.rkt` that it was backing up for the date 1992-01-01 and
then later resolve 1992-01-01 to a snapshot other than the one provided on
stdin, it may be impossible to restore the Glacier archives uploaded by
`btrsend.rkt`.

`btrsend.rkt` compresses its input with `gzip -7` and encrypts asymmetrically
(not really, symmetric + asymmetrically encrypted key) with `gpg`. To tune this
behaviour, search the source for `make-gpg-comress-pipe`.

Each complete or incremental blob is compressed, encrypted, and uploaded to
Amazon Glacier as a single archive in a streaming manner. Old archives are
eventually deleted. All activity is logged to `.btrsend.log`. An sqlite3
database of active backup archives including lineage and size is maintained in
`.btrsend.db`. The backup schedule is set up as follows:

- A backup is created on the 1st, 7th, 14, and 21st of every month.
- A complete backup is created after a chain of 11 incremental backups have
  been created from the most recent complete backup or when no complete backup
  already exists.
- When the most recent backup in a chain of incremental backups is more than 90
  days old, the entire chain is deleted.

This schedule may be adjusted by editing the constants at the top of the
`backup-planner` function. Changing them will probably break the built-in test
suite.

To run tests, `raco test btrsend.rkt`

To use `btrsend.rkt` with the scripts described above:

- Create something executable called ~/.btrsend . Make it `chdir` to your home
  directory and then `exec` `btrsend.rkt` with its own `argv`.
- Set up the scripts and required directory structure somewhere.
- Add an entry to root's crontab such that `btrsnap` is executed every
  midnight.

To get `btrsend.rkt` to read your AWS credentials see
https://github.com/greghendershott/aws .

## WARNING ##

Backups are very cheap to create and store. Backups can be *very expensive* to
restore. Read the Glacier pricing guide before attempting to restore. Retrieve
files slowly to avoid getting an absurdly high bill.
