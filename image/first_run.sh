#! /bin/bash
pip install --upgrade pip

filename='/app/requirements.txt'
if [ -e $filename ]; then
    while IFS='' read -r line || [[ -n "$line" ]]; do
        if [[ -z "$line" ]]; then
            # echo "Skip empty line in requirements.txt"
            continue
        fi
        cmd="pip install $line"

        echo "$cmd" # 顯示目前正要安裝什麼
        output=$($cmd 2>&1)
        exit_code=$?
        if [ $exit_code -eq 1 ]; then
            echo $output
            exit
        fi
    done < $filename
fi

echo "Delete cache files."
find /app/ | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf
echo "first_run finish."