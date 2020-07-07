DBPATTERN=$1
RWPATTERN=$2
JOBID=$3
python transfer.py break-data-bank break-data-bank $GOOGLE_APPLICATION_CREDENTIALS $GOOGLE_APPLICATION_CREDENTIALS /events/next2020/transactions/$JOBID/transactions /events/next2020/cached_1_6_12/$DBPATTERN/patterns/$RWPATTERN/transactions --seed=0
