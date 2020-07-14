while [ 1 ]
do
    python gather_data_runs.py
    if [ $? -eq 0 ]
    then
        break
    fi
done
