table=$1
action=$2
start_date=$3
end_date=$4
result_output_location=$5

current_date="$start_date"
while true; do
    [ "$current_date" \< "$end_date" ] || { echo "$current_date"; break; }
    echo "$current_date"

    aws athena start-query-execution \
        --query-string "ALTER TABLE $table $action PARTITION (dt='"$current_date"')" \
        --result-configuration "OutputLocation="$result_output_location",EncryptionConfiguration={EncryptionOption=SSE_S3}"
    current_date=$( date +%Y-%m-%d --date "$current_date +1 day" )
done